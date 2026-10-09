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
use crate::memtable::MemTable;
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
	/// of stale records, a switch to a fenced round, or a direct-to-L0 write).
	BeforeApplyRound {
		round: usize,
	},
	/// A fenced round holds the active memtable's read guard and has logged the batches that were
	/// stale, and synced them if the group syncs. It has not applied anything yet.
	BeforeFencedApply,
}

#[cfg(test)]
pub(crate) type PipelineHookFn = Arc<dyn Fn(PipelineHook) + Send + Sync>;

/// Observation points in a committer's path through `commit`, so tests can hold a commit at an
/// exact step while the pipeline shuts down.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommitStage {
	/// The commit passed the shutdown and write stall checks and has not asked for admission.
	Entered,
	/// The commit holds an admission permit and has not claimed a ring sequence.
	Admitted,
	/// The commit claimed its sequence and validated, and has not been accepted.
	Validated,
	/// The commit was accepted and waits for the flusher.
	Accepted,
}

#[cfg(test)]
pub(crate) type CommitHookFn = Arc<dyn Fn(CommitStage) + Send + Sync>;

/// The most bytes of batches the flusher gathers into one group, as `Batch::encoded_len_hint`
/// counts them, unless the first entry alone is bigger: that entry is a group of its own.
///
/// A group is encoded into one contiguous WAL buffer of its size, next to the batches themselves,
/// so without a bound a few concurrent multi-GiB commits need that much contiguous memory at
/// once. 4 MiB is about 4 ms of sequential write at 1 GB/s, on the order of the fsync that
/// follows it: a bigger group saves little more per commit and adds to the latency of every
/// commit in it. Groups of small commits are far below it, so only a pile-up of large commits
/// reaches the bound.
pub(crate) const MAX_GROUP_BYTES: usize = 4 << 20;

/// Whether the next entry, of `bytes`, joins a group that holds `entries` entries of
/// `group_bytes` bytes between them. A group takes at least one entry, and after that entries
/// only while it stays within `MAX_GROUP_BYTES`.
fn group_has_room(entries: usize, group_bytes: usize, bytes: usize) -> bool {
	entries == 0 || group_bytes.saturating_add(bytes) <= MAX_GROUP_BYTES
}

/// Reserves room for `bytes` bytes in `buf`, which is empty, and fails when the allocator
/// refuses, where `Vec::reserve` aborts the process. Exactly `bytes`, not the doubling that
/// `reserve` rounds up to.
///
/// Only the group's buffer is reserved this way. Wrapping a value and compressing a record
/// allocate per batch, with the ordinary allocator.
fn try_reserve_wal(buf: &mut Vec<u8>, bytes: usize) -> Result<()> {
	debug_assert!(buf.is_empty(), "a reservation made after encoding would not be exact");
	buf.try_reserve_exact(bytes).map_err(|e| Error::Io(Arc::new(std::io::Error::from(e))))
}

/// How many commits may be admitted at once: one admission permit each. At most half the
/// ring's capacity of entries then sit above the completed prefix.
pub(super) const ADMISSION_PERMITS: u32 = (DEFAULT_COMMIT_RING_CAPACITY / 2) as u32;

/// How many times in a row a group may find its records in a segment other than the active
/// memtable's tag, with nothing applied in between, and log them again off the memtable's lock.
/// One pass past a rotation fixes it. If rotations keep overtaking the group, the next pass is
/// fenced: see `CommitPipeline::apply_fenced`.
pub(crate) const UNFENCED_STALE_ROUNDS: u32 = 2;

/// How many groups the flusher drains between two calls to `retire`.
pub(crate) const RETIRE_EVERY_GROUPS: u32 = 16;

/// How many ring entries the flusher drains between two calls to `retire`, if that comes
/// before `RETIRE_EVERY_GROUPS` groups. A few huge groups would otherwise leave nearly the
/// whole ring unretired (about 0.9 KB an entry) while the flusher is idle, and there is no
/// timer to retire it later: this bounds what an idle ring holds to about this many entries.
pub(crate) const RETIRE_EVERY_ENTRIES: u64 = 1024;

/// Whether `retire` is due after the groups and entries drained since it last ran.
fn retire_due(groups: u32, entries: u64) -> bool {
	groups >= RETIRE_EVERY_GROUPS || entries >= RETIRE_EVERY_ENTRIES
}

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

/// Holds an entry its committer claimed until the committer has a verdict for it. If the
/// committer goes away first the entry is aborted, so that the flusher, which drains the
/// ring in order, is not left waiting at it forever. The window from the claim to the
/// verdict holds no await, so only a panic can end it early.
struct ClaimGuard<'a> {
	pipeline: &'a CommitPipeline,
	entry: &'a CommitEntry,
}

impl Drop for ClaimGuard<'_> {
	fn drop(&mut self) {
		if self.entry.abort_if_in_flight() {
			self.pipeline.ring.advance_completed();
			self.pipeline.notify_flusher.notify_one();
		}
	}
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
/// The flusher drains accepted entries in ring order, in groups of at most
/// `MAX_GROUP_BYTES` (a group always takes its first entry), assigns their LSM
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
	/// never waits on the flusher. At shutdown the flusher takes every permit,
	/// which tells it that no admitted commit is left, and closes the semaphore.
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
	/// Test-only observer of a committer's steps.
	#[cfg(test)]
	commit_hook: Mutex<Option<CommitHookFn>>,
	/// Test-only count of the times the flusher found nothing to drain at shutdown and began to
	/// wait for the admitted commits.
	#[cfg(test)]
	shutdown_waits: AtomicU64,
	/// Test-only observer called inside `retire`, between its two reads.
	#[cfg(test)]
	retire_gap: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
	/// Test-only failpoint: a WAL buffer of at least this many bytes is refused, as if the
	/// allocator had none to give. `usize::MAX` disables it.
	#[cfg(test)]
	wal_reserve_fails_from: std::sync::atomic::AtomicUsize,
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
		let admission = Arc::new(Semaphore::new(ADMISSION_PERMITS as usize));
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
			#[cfg(test)]
			commit_hook: Mutex::new(None),
			#[cfg(test)]
			shutdown_waits: AtomicU64::new(0),
			#[cfg(test)]
			retire_gap: Mutex::new(None),
			#[cfg(test)]
			wal_reserve_fails_from: std::sync::atomic::AtomicUsize::new(usize::MAX),
		}
	}

	/// Test observer: the completed prefix, the retired watermark and the length of the
	/// overflow map.
	#[cfg(test)]
	pub(crate) fn watermarks(&self) -> (u64, u64, usize) {
		(self.ring.completed(), self.ring.taken(), self.overflow.lock().len())
	}

	/// Test observer: how many entries above the completed prefix are accepted, whether the
	/// flusher holds them or not.
	#[cfg(test)]
	pub(crate) fn accepted_waiting(&self) -> usize {
		let (from, to) = (self.ring.completed() + 1, self.ring.published());
		(from..=to)
			.filter(|seq| {
				matches!(self.ring.get(*seq), SlotRead::Ready(e) if matches!(e.state(), EntryState::Accepted))
			})
			.count()
	}

	/// Test observer: the admission permits not held by a claimed entry.
	#[cfg(test)]
	pub(crate) fn free_permits(&self) -> usize {
		self.admission.available_permits()
	}

	/// Makes every WAL buffer of at least `bytes` bytes fail to allocate, as it would with no
	/// memory left. `usize::MAX` lifts it.
	#[cfg(test)]
	pub(crate) fn set_wal_reserve_fails_from(&self, bytes: usize) {
		self.wal_reserve_fails_from.store(bytes, Ordering::Relaxed);
	}

	/// Installs (or clears) the observer called inside `retire` after it read the completed
	/// prefix and before it scans the pinned transactions.
	#[cfg(test)]
	pub(crate) fn set_retire_gap_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
		*self.retire_gap.lock() = hook;
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

	/// Installs (or clears) the observer called at each `CommitStage`.
	#[cfg(test)]
	pub(crate) fn set_commit_hook(&self, hook: Option<CommitHookFn>) {
		*self.commit_hook.lock() = hook;
	}

	#[cfg(test)]
	fn fire_commit(&self, stage: CommitStage) {
		let hook = self.commit_hook.lock().clone();
		if let Some(hook) = hook {
			hook(stage);
		}
	}

	/// How many times the flusher began to wait, at shutdown, for the admitted commits.
	#[cfg(test)]
	pub(crate) fn shutdown_waits(&self) -> u64 {
		self.shutdown_waits.load(Ordering::SeqCst)
	}

	/// Starts the background flusher task.
	pub(crate) fn start_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
		let pipeline = Arc::clone(self);
		// The flusher's position is read here, not when the task is first polled. A commit that
		// fails before then has its entry aborted, and its own guard advances the completed
		// prefix over it. A flusher that started past that entry would never take its permit,
		// and `close` waits for every permit.
		let drained = self.ring.completed();
		tokio::spawn(async move {
			pipeline.run_flusher(drained).await;
		})
	}

	/// The value a mutating transaction pins in `active_txn_tracker` at
	/// begin. Read before [`commit_window`](Self::commit_window), so the
	/// retired watermark can never pass the window it then reads.
	///
	/// The pin and the window are two reads of the same monotonic completed
	/// prefix, the pin first, so the pin is at most the window. `retire` reads
	/// the completed prefix before it scans the pins, so the watermark stays
	/// at or below every live window: a pin the scan sees bounds it by at
	/// most that window, and a transaction the scan misses registers after it
	/// and so reads its window from a prefix at least as high.
	///
	/// The pin must not be read from the retired watermark. `retire` sets the
	/// watermark to the oldest pin (or the completed prefix, if lower), and a
	/// pin read from the watermark is never above it, so while any mutating
	/// transaction is live the watermark cannot move. Every lapped
	/// `CommitEntry` then goes into the `overflow` map under its mutex (see
	/// `publish`) and memory grows by about 0.7-0.8 KB per commit.
	pub(crate) fn commit_pin(&self) -> u64 {
		self.ring.completed()
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
		if self.shutdown.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}
		let read_bloom = bloom_of(read_keys);
		let permit =
			Arc::clone(&self.admission).acquire_owned().await.map_err(|_| Error::PipelineStall)?;
		// The claimed sequence bounds the window; the entry publishes nothing, so the guard
		// aborts it once it is validated.
		let entry = Arc::new(CommitEntry::new(Vec::new(), permit));
		let seq = self.publish(&entry);
		let _claim = ClaimGuard {
			pipeline: self,
			entry: &entry,
		};
		self.validate(&entry, seq, start_seq, window, read_keys, &read_bloom)
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

		#[cfg(test)]
		self.fire_commit(CommitStage::Entered);

		// The last suspension point before the entry is claimed: from here to
		// the verdict nothing awaits, so a cancelled commit can never leave a
		// claimed entry unpublished or undecided.
		let permit =
			Arc::clone(&self.admission).acquire_owned().await.map_err(|_| Error::PipelineStall)?;

		// Check if restore is in progress
		if self.restoring.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}
		#[cfg(test)]
		self.fire_commit(CommitStage::Admitted);

		let write_keys: Vec<Key> = batch.entries.iter().map(|e| e.key.clone()).collect();
		let read_bloom = bloom_of(read_set);
		let entry = Arc::new(CommitEntry::new(write_keys, permit));
		let seq = self.publish(&entry);

		// A conflict aborts the entry, and so does a committer that fails before its verdict.
		let claim = ClaimGuard {
			pipeline: self,
			entry: &entry,
		};
		self.validate(&entry, seq, start_seq, window, read_set, &read_bloom)?;

		#[cfg(test)]
		self.fire_commit(CommitStage::Validated);
		let (complete_tx, complete_rx) = oneshot::channel();
		entry.accept(Payload {
			batch,
			sync,
			complete_tx,
			epoch: self.restore_epoch.load(Ordering::SeqCst),
		});
		// The entry is decided, and the flusher takes it from here.
		drop(claim);
		self.notify_flusher.notify_one();
		#[cfg(test)]
		self.fire_commit(CommitStage::Accepted);

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
	async fn run_flusher(&self, mut drained: u64) {
		// `drained` is the last ring sequence the flusher has consumed.
		// Groups flushed, and ring entries drained, since `retire` last ran.
		let mut groups_since_retire = 0u32;
		let mut entries_since_retire = 0u64;
		// Completes once every admission permit is back, which is once no commit is admitted
		// and undecided. Only polled at shutdown.
		let mut all_permits =
			std::pin::pin!(Arc::clone(&self.admission).acquire_many_owned(ADMISSION_PERMITS));
		loop {
			// Gather the contiguous decided entries after `drained`, with the permits to
			// release once the completed prefix passes them. The group stops before the entry
			// that would take it past `MAX_GROUP_BYTES`. That entry is still accepted and in the
			// ring, and starts the next group: the flusher parks only when a pass finds nothing,
			// and a group always takes its first entry.
			let mut group: Vec<(Arc<CommitEntry>, Payload)> = Vec::new();
			let mut permits = Vec::new();
			let mut group_bytes = 0usize;
			let mut next = drained + 1;
			loop {
				match self.ring.get(next) {
					SlotRead::Ready(entry) => match entry.state() {
						EntryState::InFlight => break,
						EntryState::Accepted => {
							let bytes = entry.payload_bytes();
							if !group_has_room(group.len(), group_bytes, bytes) {
								break;
							}
							group_bytes = group_bytes.saturating_add(bytes);
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
				if self.shutdown.load(Ordering::Acquire) {
					#[cfg(test)]
					self.shutdown_waits.fetch_add(1, Ordering::SeqCst);
					// Nothing to drain, but a commit that was admitted before the shutdown may
					// still be on its way to the ring. A permit is held from admission until
					// the flusher has drained the entry, so once every permit is back, all of
					// them were decided and no more can arrive.
					tokio::select! {
						all = &mut all_permits => {
							// Commits still waiting for a permit fail instead of getting one.
							self.admission.close();
							drop(all);
							break;
						}
						() = self.notify_flusher.notified() => {}
					}
					continue;
				}
				// Wait for new published or decided entries
				self.notify_flusher.notified().await;
				continue;
			}
			let entries = next - (drained + 1);
			entries_since_retire += entries;
			drained = next - 1;

			if !group.is_empty() {
				self.flush_entries(group).await;
			}
			// Every drained entry is now complete, so the completed prefix
			// covers them and their permits may admit new claims.
			self.ring.advance_completed();
			drop(permits);
			// Retirement only frees memory, and scanning the transaction pins costs
			// far more than the rest of a small group, so do it every few groups, or
			// sooner after a large number of entries. The watermark then lags, which is safe:
			// `publish` keeps more lapped entries in the overflow map and `validate`
			// treats a missing one as a conflict. There is no retire when the flusher
			// parks, since that would bring back one per group for a lone writer.
			groups_since_retire += 1;
			if retire_due(groups_since_retire, entries_since_retire) {
				groups_since_retire = 0;
				entries_since_retire = 0;
				self.retire();
			}
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
				Err(e) => Err(Error::Io(
					std::io::Error::other(format!("Group commit failed: {e}")).into(),
				)),
			});
		}
	}

	/// Advances the retired watermark to the oldest pin, or to the completed prefix if that is
	/// lower. A transaction that read its pin before the scan and registers after it is not
	/// seen, so the watermark may pass its pin, but never its window: the scan follows the read
	/// of the completed prefix that bounds it, and that transaction reads its window from a
	/// prefix at least as high.
	///
	/// Drops the overflow entries that no live window can reach any more.
	fn retire(&self) {
		// Read the completed prefix before scanning the pins: a transaction
		// that registers after the scan reads its window after this value.
		let completed = self.ring.completed();
		#[cfg(test)]
		{
			let hook = self.retire_gap.lock().clone();
			if let Some(hook) = hook {
				hook();
			}
		}
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
		// A refused reservation fails the group here, before anything is appended to the WAL.
		let mut wal_buf = Vec::new();
		self.reserve_wal_buf(
			&mut wal_buf,
			processed_batches.iter().map(Batch::encoded_len_hint).sum(),
		)?;
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
		// A rotation can come between the append and the apply again. A group that was
		// overtaken `UNFENCED_STALE_ROUNDS` times in a row appends the rest again under
		// the read lock and applies it there, where no rotation can interrupt: a group
		// whose records are logged is not failed for want of a quiet moment.
		let mut at = 0;
		let mut stale_rounds = 0;
		let mut fenced = false;
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

			let (next, stop) = if fenced {
				self.apply_fenced(
					&processed_batches,
					&oversized,
					&mut segments,
					at,
					sync,
					&mut wal_buf,
					&mut wal_ends,
				)?
			} else {
				self.apply_run(&processed_batches, &oversized, &segments, at)?
			};
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
					// A fenced round logs the stale batches under the guard that fixes the tag,
					// so it is never stale. A tag that the WAL cannot have is not a race either:
					// no rotation will bring them together. Fail the group rather than log it
					// again.
					if fenced || !self.tag_is_possible(&segments[at..], active_tag) {
						return Err(tag_mismatch(&segments[at..], active_tag));
					}
					if stale_rounds == UNFENCED_STALE_ROUNDS {
						fenced = true;
					} else {
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
		}

		Ok(())
	}

	/// Whether `tag`, which the active memtable had a moment ago, can be explained by the WAL: a
	/// rotation only moves the WAL on, and the tag with it, so the tag is neither behind a segment
	/// that a record was logged in nor ahead of the active segment.
	fn tag_is_possible(&self, segments: &[u64], tag: u64) -> bool {
		segments.iter().all(|&segment| segment <= tag)
			&& tag <= self.inner.wal.read().get_active_log_number()
	}

	/// Reserves room for `bytes` bytes in the empty `buf`, see `try_reserve_wal`.
	fn reserve_wal_buf(&self, buf: &mut Vec<u8>, bytes: usize) -> Result<()> {
		#[cfg(test)]
		{
			let fails_from = self.wal_reserve_fails_from.load(Ordering::Relaxed);
			if fails_from != usize::MAX && bytes >= fails_from {
				return Err(Error::Io(Arc::new(std::io::ErrorKind::OutOfMemory.into())));
			}
		}
		try_reserve_wal(buf, bytes)
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
		Self::apply_to(&active, batches, oversized, segments, from)
	}

	/// Like [`apply_run`](Self::apply_run), but first logs again the batches whose record is not in
	/// the segment the memtable is tagged with (see `encode_stale`), and holds the read guard from
	/// there until the batches are applied.
	///
	/// A rotation takes the write guard and then the WAL lock, so under the read guard the WAL's
	/// active segment is the memtable's tag and stays so: what is logged now is in the segment
	/// the batches are applied to, and nothing can make it stale first. A group that rotations
	/// keep overtaking makes progress this way. Not every group does it, because the guard is
	/// held across an append and an fsync, which run on the flusher's thread instead of the
	/// pool's. A group that syncs is synced before anything of it is applied, as everywhere else.
	///
	/// If the active segment is not the tag, no rotation will bring them together, and the group
	/// fails before anything more is logged.
	///
	/// Synchronous like `apply_run`, for the same reason.
	#[allow(clippy::too_many_arguments)]
	fn apply_fenced(
		&self,
		batches: &[Batch],
		oversized: &[bool],
		segments: &mut [u64],
		from: usize,
		sync: bool,
		wal_buf: &mut Vec<u8>,
		wal_ends: &mut Vec<usize>,
	) -> Result<(usize, ApplyStop)> {
		let active = self.inner.active_memtable.read()?;
		let tag = active.get_wal_number();
		let stale = encode_stale(
			&batches[from..],
			&oversized[from..],
			&segments[from..],
			tag,
			wal_buf,
			wal_ends,
		)?;
		if !stale.is_empty() {
			let mut wal = self.inner.wal.write();
			if wal.get_active_log_number() != tag {
				return Err(tag_mismatch(&segments[from..], tag));
			}
			let segment = wal.append_group(wal_buf, wal_ends)?;
			if sync {
				wal.sync()?;
			}
			for j in stale {
				segments[from + j] = segment;
			}
		}
		#[cfg(test)]
		self.fire(PipelineHook::BeforeFencedApply);
		Self::apply_to(&active, batches, oversized, segments, from)
	}

	/// The loop of `apply_run`, on the memtable the caller holds the read guard of.
	fn apply_to(
		active: &MemTable,
		batches: &[Batch],
		oversized: &[bool],
		segments: &[u64],
		from: usize,
	) -> Result<(usize, ApplyStop)> {
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
	/// encoded into the group's own buffers, which the first append gave back with room for the
	/// whole group. Nothing is reserved here, because part of the group is already applied and
	/// the step must not fail for want of memory.
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

	/// Shuts down the pipeline. New commits fail at once. The flusher decides every commit that
	/// was admitted before, making the accepted ones durable, and only then exits. A commit that
	/// was handed a permit while it queued for admission, and whose future is then neither polled
	/// nor dropped, keeps the flusher waiting for it.
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
/// The buffers are the group's own, which the first append gave back with room for the whole
/// group, so nothing is reserved: part of the group may be applied already, and the step must not
/// fail for want of memory.
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

/// The error of a group whose records are in segments that no rotation will make the active
/// memtable's tag: the tag is wrong, and logging the group again would not help.
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

#[cfg(test)]
mod retire_due_tests {
	use super::{retire_due, RETIRE_EVERY_ENTRIES, RETIRE_EVERY_GROUPS};

	#[test]
	fn retire_is_due_after_enough_groups_or_enough_entries() {
		assert!(!retire_due(0, 0));
		// A lone writer: 15 groups of one entry is not yet time.
		assert!(!retire_due(RETIRE_EVERY_GROUPS - 1, u64::from(RETIRE_EVERY_GROUPS - 1)));
		assert!(retire_due(RETIRE_EVERY_GROUPS, u64::from(RETIRE_EVERY_GROUPS)));
		// A few huge groups: due on entries alone, well before the group count.
		assert!(!retire_due(2, RETIRE_EVERY_ENTRIES - 1));
		assert!(retire_due(2, RETIRE_EVERY_ENTRIES));
		assert!(retire_due(1, RETIRE_EVERY_ENTRIES + 1));
	}

	#[test]
	fn the_usual_group_sizes_do_not_retire_more_often_than_every_sixteen_groups() {
		// 64 writers form groups of about 30 entries; 16 of them stay under the entry limit,
		// so the entry trigger changes nothing at ordinary load.
		assert!(u64::from(RETIRE_EVERY_GROUPS) * 64 <= RETIRE_EVERY_ENTRIES);
		assert!(!retire_due(RETIRE_EVERY_GROUPS - 1, u64::from(RETIRE_EVERY_GROUPS - 1) * 64));
	}
}
