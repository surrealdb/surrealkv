use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore};

use super::bloom::BloomFilter;
use super::commit_ring::{CommitRing, SlotRead, DEFAULT_COMMIT_RING_CAPACITY};
use super::queue::{CommitEntry, EntryState, Payload};
use super::sync::backoff;
use crate::batch::{Batch, MAX_BATCH_SIZE};
use crate::error::{Error, Result};
use crate::lsm::CoreInner;
use crate::memtable::MemTable;
use crate::stall::WriteStallController;
use crate::storage::{AffinityLogStore, LogStore};
use crate::task::TaskManager;
use crate::varint::varint_len_u64;
use crate::vlog::{VLog, ValueLocation, VALUE_LOCATION_VERSION};
use crate::{InternalKey, InternalKeyKind, Key};

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

/// Buffers the flusher keeps from one group to the next, so a group of one commit does not
/// allocate and free a dozen of them. The flusher is the only user, so there is one set, every
/// buffer has one owner, and nothing is shared.
#[derive(Default)]
struct FlushScratch {
	/// The decided entries drained from the ring, with their payloads.
	group: Vec<(Arc<CommitEntry>, Payload)>,
	/// The entries of `batches`, each with the highest sequence number it was assigned.
	entries: Vec<(Arc<CommitEntry>, u64)>,
	/// The batches of the group, in ring order. A batch that goes to a memtable is spent.
	batches: Vec<Batch>,
	/// Where the outcome of each batch goes.
	waiters: Vec<oneshot::Sender<Result<()>>>,
	/// The worst-case memtable size of each batch, once its values are encoded.
	estimates: Vec<u64>,
	/// Whether each batch of the group is too big for a memtable.
	oversized: Vec<bool>,
	/// The segment each batch of the group was logged in.
	segments: Vec<u64>,
	/// The group's WAL records, one after the other.
	wal_buf: Vec<u8>,
	/// Where each record in `wal_buf` ends.
	wal_ends: Vec<usize>,
	/// Whether a batch of the group is in a memtable or an L0 table already. A failure from then
	/// on cannot leave nothing of the group behind.
	applied: bool,
	/// Retired overflow entries between leaving the map and being dropped. Empty between steps,
	/// and at most `RETIRED_FREE_CHUNK` long.
	retired: Vec<Arc<CommitEntry>>,
}

/// The most capacity a vector of `FlushScratch` is kept at between groups, in elements. A burst of
/// commits does not pin its memory for the life of the tree.
const MAX_SCRATCH_ELEMENTS: usize = 32 * 1024;

/// The same for the WAL buffer, in bytes: a burst of large batches does not pin its memory either.
///
/// A quarter over `MAX_GROUP_BYTES`. Wrapping a value adds two bytes to what the gather counted,
/// and an entry counts for 14 bytes at the least, so a group at the budget reserves at most
/// 4.57 MiB (a seventh over). A cap below that frees the buffer after every group of the
/// smallest entries, and the quarter keeps the most a tree holds on to at 5 MiB. The buffer is
/// grown to what a group needs and no more, so what is kept is the largest group seen.
const MAX_SCRATCH_BYTES: usize = MAX_GROUP_BYTES + MAX_GROUP_BYTES / 4;

impl FlushScratch {
	/// Empties every buffer for the next group, and gives back the allocation of one that a
	/// burst grew past what is worth keeping.
	fn recycle(&mut self) {
		recycle(&mut self.group, MAX_SCRATCH_ELEMENTS);
		recycle(&mut self.entries, MAX_SCRATCH_ELEMENTS);
		recycle(&mut self.batches, MAX_SCRATCH_ELEMENTS);
		recycle(&mut self.waiters, MAX_SCRATCH_ELEMENTS);
		recycle(&mut self.estimates, MAX_SCRATCH_ELEMENTS);
		recycle(&mut self.oversized, MAX_SCRATCH_ELEMENTS);
		recycle(&mut self.segments, MAX_SCRATCH_ELEMENTS);
		recycle(&mut self.wal_buf, MAX_SCRATCH_BYTES);
		recycle(&mut self.wal_ends, MAX_SCRATCH_ELEMENTS);
		self.applied = false;
	}
}

/// Empties `buffer`, and drops its allocation if it has room for more than `max_capacity`
/// elements.
fn recycle<T>(buffer: &mut Vec<T>, max_capacity: usize) {
	buffer.clear();
	if buffer.capacity() > max_capacity {
		*buffer = Vec::new();
	}
}

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
/// `reserve` rounds up to, so the buffer stays within what `MAX_SCRATCH_BYTES` keeps.
///
/// Only the group's buffer is reserved this way. Wrapping a value and compressing a record
/// allocate per batch, with the ordinary allocator.
fn try_reserve_wal(buf: &mut Vec<u8>, bytes: usize) -> Result<()> {
	debug_assert!(buf.is_empty(), "a reservation made after encoding would not be exact");
	buf.try_reserve_exact(bytes).map_err(|e| Error::Io(Arc::new(std::io::Error::from(e))))
}

/// Folds `permit` into `merged`, so that a group gives its admission permits back with one
/// release, one acquisition of the semaphore's lock, rather than one per entry.
fn merge_permit(merged: &mut Option<OwnedSemaphorePermit>, permit: Option<OwnedSemaphorePermit>) {
	if let Some(permit) = permit {
		match merged {
			Some(merged) => merged.merge(permit),
			None => *merged = Some(permit),
		}
	}
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

/// How many retired entries the flusher frees between two groups, from the overflow map, and at
/// least that many from the ring. A transaction that stayed open for many laps of the ring leaves
/// the map holding every entry the ring overwrote meanwhile, and the ring itself holds a lap of
/// them once it ends. Freeing one costs a fraction of a microsecond, so freeing them all at once
/// would stall every commit behind the flusher for as long as they are many. They go in chunks
/// instead, and the flusher keeps going while it has nothing else to do.
pub(crate) const RETIRED_FREE_CHUNK: usize = 256;

/// How many ring slots the flusher releases after a group that drained `entries` entries. A
/// retire passes as many slots as the groups since the last one drained, so releasing a fixed
/// chunk per group would fall behind from about `RETIRED_FREE_CHUNK` entries a group and leave
/// up to the whole ring holding retired entries for as long as the load lasts. Twice the group's
/// own size keeps up, and costs a small part of what the group costs itself.
fn retired_slots_per_step(entries: u64) -> u64 {
	(RETIRED_FREE_CHUNK as u64).max(entries.saturating_mul(2))
}

/// Removes up to `limit` entries of `overflow` whose sequence is at or below `taken`, lowest
/// first, and drops them after the mutex is released. Returns how many it dropped and whether
/// more are left at or below `taken`. `freed` is the buffer they pass through, empty on entry.
///
/// The entries at or below `taken` are out of every live window's reach and nothing reads them,
/// so leaving some for a later call is safe, and none above `taken` is ever removed.
fn free_retired_from<V>(
	overflow: &Mutex<BTreeMap<u64, V>>,
	taken: u64,
	limit: usize,
	freed: &mut Vec<V>,
) -> (usize, bool) {
	let more = {
		let mut overflow = overflow.lock();
		while freed.len() < limit {
			match overflow.first_entry() {
				Some(entry) if *entry.key() <= taken => freed.push(entry.remove()),
				_ => break,
			}
		}
		overflow.first_key_value().is_some_and(|(seq, _)| *seq <= taken)
	};
	let count = freed.len();
	freed.clear();
	(count, more)
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
	/// The memtable took part of the next batch and then failed, which it only does if its size
	/// estimate drifted from what the batch needs.
	Torn(Error),
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
	/// transactions, whose window spans more than a lap, ever read it. Entries
	/// at or below the retired watermark are garbage, which `free_retired`
	/// removes a chunk at a time.
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
	/// Test-only observer called at the start of every `free_retired`.
	#[cfg(test)]
	free_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
	/// Test-only record of what `free_retired` freed.
	#[cfg(test)]
	free_stats: Mutex<FreeStats>,
}

/// What `free_retired` has done so far.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FreeStats {
	/// The calls that freed at least one entry.
	pub(crate) steps: usize,
	/// The entries freed in all.
	pub(crate) total: usize,
	/// The most entries one call freed.
	pub(crate) most: usize,
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
			#[cfg(test)]
			free_hook: Mutex::new(None),
			#[cfg(test)]
			free_stats: Mutex::new(FreeStats::default()),
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

	/// Test observer: whether the entry at `seq` has been accepted and waits for the flusher.
	#[cfg(test)]
	pub(crate) fn is_accepted(&self, seq: u64) -> bool {
		matches!(
			self.ring.read(seq, |entry| matches!(entry.state(), EntryState::Accepted)),
			SlotRead::Ready(true)
		)
	}

	/// Test observer: how many entries of the overflow map the retired watermark has passed.
	#[cfg(test)]
	pub(crate) fn retired_in_overflow(&self) -> usize {
		self.overflow.lock().range(..=self.ring.taken()).count()
	}

	/// Test observer: panics if the ring lapped an entry above the retired watermark that the
	/// overflow map does not hold. One that is missing and at or below the watermark by the time
	/// it is looked for was freed legitimately.
	#[cfg(test)]
	pub(crate) fn assert_lapped_entries_are_kept(&self) {
		let lapped = self.ring.published().saturating_sub(self.ring.capacity());
		for seq in (self.ring.taken() + 1)..=lapped {
			// Never hold the map's mutex while reading a slot: `publish` takes them the other
			// way round.
			if matches!(self.ring.read(seq, |_| ()), SlotRead::Gone)
				&& !self.overflow.lock().contains_key(&seq)
			{
				let taken = self.ring.taken();
				assert!(seq <= taken, "lapped entry {seq} above the watermark {taken} was freed");
			}
		}
	}

	/// Installs (or clears) the observer called inside `retire` after it read the completed
	/// prefix and before it scans the pinned transactions.
	#[cfg(test)]
	pub(crate) fn set_retire_gap_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
		*self.retire_gap.lock() = hook;
	}

	/// Installs (or clears) the observer called at the start of every `free_retired`, before it
	/// takes anything out of the overflow map.
	#[cfg(test)]
	pub(crate) fn set_free_retired_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
		*self.free_hook.lock() = hook;
	}

	/// What `free_retired` has freed so far.
	#[cfg(test)]
	pub(crate) fn free_stats(&self) -> FreeStats {
		*self.free_stats.lock()
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
		let mut scratch = FlushScratch::default();
		// Groups flushed, and ring entries drained, since `retire` last ran.
		let mut groups_since_retire = 0u32;
		let mut entries_since_retire = 0u64;
		// Completes once every admission permit is back, which is once no commit is admitted
		// and undecided. Only polled at shutdown.
		let mut all_permits =
			std::pin::pin!(Arc::clone(&self.admission).acquire_many_owned(ADMISSION_PERMITS));
		// Whether retired entries may still be waiting in the overflow map.
		let mut retired_pending = false;
		loop {
			// Gather the contiguous decided entries after `drained`, with the permits to
			// release once the completed prefix passes them, as one. The group stops before
			// the entry that would take it past `MAX_GROUP_BYTES`. That entry is still accepted
			// and in the ring, and starts the next group: the flusher parks only when a pass
			// finds nothing, and a group always takes its first entry.
			let mut permits: Option<OwnedSemaphorePermit> = None;
			let mut group_bytes = 0usize;
			let mut next = drained + 1;
			loop {
				match self.ring.get(next) {
					SlotRead::Ready(entry) => match entry.state() {
						EntryState::InFlight => break,
						EntryState::Accepted => {
							let bytes = entry.payload_bytes();
							if !group_has_room(scratch.group.len(), group_bytes, bytes) {
								break;
							}
							group_bytes = group_bytes.saturating_add(bytes);
							let payload =
								entry.take_payload().expect("accepted entries carry a payload");
							merge_permit(&mut permits, entry.take_permit());
							scratch.group.push((entry, payload));
						}
						EntryState::Complete => merge_permit(&mut permits, entry.take_permit()),
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
				if retired_pending {
					// Nothing to flush: carry on freeing instead of parking with the
					// memory held, yielding so that the committers get to run.
					retired_pending = self.free_retired(&mut scratch.retired, 0);
					tokio::task::yield_now().await;
					continue;
				}
				// Wait for new published or decided entries
				self.notify_flusher.notified().await;
				continue;
			}
			let entries = next - (drained + 1);
			entries_since_retire += entries;
			drained = next - 1;

			if !scratch.group.is_empty() {
				self.flush_entries(&mut scratch).await;
				scratch.recycle();
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
				retired_pending = true;
			}
			// One chunk per group, so that the entries a long-lived transaction left behind
			// do not stall the commits that follow it.
			if retired_pending {
				retired_pending = self.free_retired(&mut scratch.retired, entries);
			}
		}
	}

	/// Assigns LSM sequence numbers to a group of accepted entries in ring
	/// order, makes them durable and visible, and completes their commits. Drains
	/// `scratch.group`, and leaves the rest of `scratch` for the caller to recycle.
	///
	/// A group that fails before any of it is applied fails whole and leaves nothing behind. One
	/// that fails after part of it was applied cannot be taken back: the batches in the memtables
	/// would become readable with the next group that publishes. It stops the database instead,
	/// see `stop_database`, and no sequence number past it is ever published.
	async fn flush_entries(&self, scratch: &mut FlushScratch) {
		let epoch = self.restore_epoch.load(Ordering::SeqCst);
		// A stopped database commits nothing more: entries accepted before it stopped fail like
		// the commits that come after.
		let stopped = self.inner.error_handler.check_error().err();
		// Out of the scratch while `flush_group_with` borrows the rest of it.
		let mut batches = std::mem::take(&mut scratch.batches);
		let mut need_sync = false;
		for (entry, payload) in scratch.group.drain(..) {
			let Payload {
				mut batch,
				sync,
				complete_tx,
				epoch: accepted_in,
			} = payload;
			if let Some(error) = &stopped {
				entry.abort();
				let _ = complete_tx.send(Err(error.clone()));
				continue;
			}
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
			scratch.entries.push((entry, start + count - 1));
			batches.push(batch);
			scratch.waiters.push(complete_tx);
		}
		if batches.is_empty() {
			scratch.batches = batches;
			return;
		}

		let result = match self.flush_group_with(&mut batches, need_sync, scratch).await {
			Err(cause) if scratch.applied => Err(self.stop_database(&cause)),
			result => {
				self.inner.group_wal_pin.store(u64::MAX, Ordering::Release);
				result
			}
		};
		let applied = result.is_ok() && self.restore_epoch.load(Ordering::SeqCst) == epoch;
		if applied {
			// Visibility first: an entry may only be marked visible, and so
			// fall below a new transaction's window, once `visible_seq_num`
			// covers it.
			let highest = scratch.entries.last().map(|(_, max)| *max).unwrap_or_default();
			self.inner.visible_seq_num.store(highest, Ordering::SeqCst);
			for (entry, max) in &scratch.entries {
				entry.make_visible(*max);
			}
		} else {
			if let Err(e) = &result {
				tracing::error!("Error during group commit flush: {:?}", e);
			}
			for (entry, _) in &scratch.entries {
				entry.abort();
			}
		}
		for complete_tx in scratch.waiters.drain(..) {
			let _ = complete_tx.send(match &result {
				Ok(()) if applied => Ok(()),
				Ok(()) => Err(Error::PipelineStall),
				Err(e @ Error::DatabaseStopped(_)) => Err(e.clone()),
				Err(e) => Err(Error::Io(
					std::io::Error::other(format!("Group commit failed: {e}")).into(),
				)),
			});
		}
		scratch.batches = batches;
	}

	/// Stops the database after a group failed with `cause` once part of it was applied, and
	/// returns the error that tells every committer of the group so.
	///
	/// What was applied stays in the memtables, where the next group that publishes would make it
	/// readable, so nothing is published past the group: the error is recorded as a hard
	/// background error, which fails every commit that comes after, and the flusher fails the ones
	/// that were accepted already. A reader keeps seeing what was visible, and recovery decides
	/// the outcome of the group when the database is opened again. The WAL segment that holds
	/// the group's records stays pinned (`CoreInner::group_wal_pin`), so recovery finds the
	/// group whole or not at all. The memtables are not flushed and no table is compacted from
	/// here on, see `BackgroundErrorHandler::commit_group_error`.
	///
	/// Writers stalled for a flush or a compaction are woken, as they are when one fails: the
	/// stopped database runs neither.
	///
	/// A restore replaces the memtables and the WAL, and so drops what the group applied, but it
	/// does not lift the stop: commits fail until the database is opened again.
	fn stop_database(&self, cause: &Error) -> Error {
		let error = Error::DatabaseStopped(format!(
			"a commit group failed after part of it was applied, and whether its commits took \
			 effect is decided by recovery when the database is opened again: {cause}"
		));
		self.inner.error_handler.stop_commit_group(error.clone());
		self.write_stall.signal_shutdown();
		error
	}

	/// Advances the retired watermark to the oldest pin, or to the completed prefix if that is
	/// lower. A transaction that read its pin before the scan and registers after it is not
	/// seen, so the watermark may pass its pin, but never its window: the scan follows the read
	/// of the completed prefix that bounds it, and that transaction reads its window from a
	/// prefix at least as high.
	///
	/// The ring slots and overflow entries it passes are left for `free_retired`.
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
		self.ring.raise_taken(bound);
	}

	/// Frees one chunk of the ring slots and one chunk of the overflow entries that the retired
	/// watermark has passed, and reports whether more of either are left. `entries` is how many
	/// ring entries the group before it drained, which scales the ring's chunk, and `freed` is
	/// the buffer the overflow entries go through, empty on entry.
	///
	/// What validation reads is above the watermark, so which of the entries at or below it are
	/// still held does not matter to it, and the rest can wait for the next call.
	fn free_retired(&self, freed: &mut Vec<Arc<CommitEntry>>, entries: u64) -> bool {
		#[cfg(test)]
		{
			let hook = self.free_hook.lock().clone();
			if let Some(hook) = hook {
				hook();
			}
		}
		let more_slots = self.ring.release_retired(retired_slots_per_step(entries));
		#[cfg_attr(not(test), allow(unused_variables))]
		let (count, more) =
			free_retired_from(&self.overflow, self.ring.taken(), RETIRED_FREE_CHUNK, freed);
		#[cfg(test)]
		if count > 0 {
			let mut stats = self.free_stats.lock();
			stats.steps += 1;
			stats.total += count;
			stats.most = stats.most.max(count);
		}
		more_slots || more
	}

	/// Flushes a group of batches to WAL and applies them to the Memtable, with scratch
	/// buffers of its own: the flusher keeps its buffers from group to group.
	///
	/// Works on copies, so the caller's batches stay intact. The flusher itself calls
	/// [`flush_group_with`](Self::flush_group_with), which spends its batches.
	#[cfg(test)]
	pub(crate) async fn flush_group(&self, batches: &[Batch], sync: bool) -> Result<()> {
		let mut owned = batches.to_vec();
		let result = self.flush_group_with(&mut owned, sync, &mut FlushScratch::default()).await;
		self.inner.group_wal_pin.store(u64::MAX, Ordering::Release);
		result
	}

	/// Flushes a group of batches to WAL and applies them to the Memtable. Encodes the values
	/// of every batch in place, and moves the keys and values of a batch that goes to a memtable
	/// into it: such a batch is spent, and only `count()` is meaningful afterwards.
	///
	/// A batch is spent only when it is applied, and the batches are applied in order, so the
	/// ones from the first that is not applied on are whole. Nothing reads a batch that was
	/// applied, and a batch that is not applied (one written to L0, one of a failed group)
	/// keeps its keys and values.
	///
	/// `scratch.applied` is set as soon as a batch is in a memtable or an L0 table. A failure
	/// before that leaves nothing of the group behind; the caller stops the database for one
	/// after it, see `flush_entries`.
	async fn flush_group_with(
		&self,
		batches: &mut [Batch],
		sync: bool,
		scratch: &mut FlushScratch,
	) -> Result<()> {
		let epoch = self.restore_epoch.load(Ordering::SeqCst);

		// 1. Process batches: separate large values to VLog if enabled (WiscKey WAL bypass)
		// and encode for WAL
		let vlog_threshold = self.inner.opts.vlog_value_threshold;
		let vlog = self.inner.vlog.as_ref();

		for batch in batches.iter_mut() {
			encode_values(batch, vlog, vlog_threshold)?;
		}

		// The values reach the OS before any record that points to them, and a group that syncs
		// has them on disk first. Its records are then never logged ahead of their values, and a
		// failed sync fails the group before the WAL has been touched.
		if let Some(vlog_inst) = vlog {
			if sync {
				vlog_inst.sync()?;
			} else {
				vlog_inst.flush()?;
			}
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
		let FlushScratch {
			estimates,
			oversized,
			segments,
			wal_buf,
			wal_ends,
			applied,
			..
		} = scratch;
		// The worst-case memtable size of each batch, computed once for the oversized test, the
		// rotation check and the apply.
		let max_memtable_size = self.inner.opts.max_memtable_size as u64;
		estimates.clear();
		estimates.extend(batches.iter().map(|batch| batch.memtable_size_estimate()));
		oversized.clear();
		oversized.extend(estimates.iter().map(|bytes| *bytes > max_memtable_size));

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

		let n = batches.len();
		wal_buf.clear();
		wal_ends.clear();
		// A refused reservation fails the group here, before anything is appended to the WAL.
		self.reserve_wal_buf(wal_buf, batches.iter().map(Batch::encoded_len_hint).sum())?;
		for batch in batches.iter() {
			batch.encode_into(wal_buf)?;
			wal_ends.push(wal_buf.len());
		}
		// One lock, one hand-off to the pool and one write for the group, and still one WAL
		// record per batch. The WAL only rotates under the lock the append holds, so the
		// whole group is in one segment. The buffers go to the pool and come back, and are
		// dropped instead if the append fails.
		let (segment, buf, ends) = self
			.log_store
			.append_group_returning_segment(std::mem::take(wal_buf), std::mem::take(wal_ends))
			.await?;
		segments.clear();
		segments.resize(n, segment);
		*wal_buf = buf;
		*wal_ends = ends;
		if sync {
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
			// A flush must not move `log_number` past the oldest segment that holds a record of
			// a batch that is not applied yet: once part of the group is applied, that record
			// may be the only copy.
			self.inner.group_wal_pin.store(oldest_segment(&segments[at..]), Ordering::Release);
			#[cfg(test)]
			{
				self.fire(PipelineHook::BeforeApplyRound {
					round,
				});
				round += 1;
			}

			let (next, stop) = if fenced {
				self.apply_fenced(
					batches, estimates, oversized, segments, at, sync, wal_buf, wal_ends,
				)?
			} else {
				self.apply_run(batches, estimates, oversized, segments, at)?
			};
			if next > at {
				stale_rounds = 0;
				*applied = true;
			}
			at = next;
			match stop {
				ApplyStop::Done => {}
				ApplyStop::Torn(e) => {
					*applied = true;
					return Err(e);
				}
				// Bypass the memtable: the batch exceeds `max_memtable_size`, or it cannot
				// fit even an empty memtable (rotating an empty memtable is a no-op, so
				// retrying would never end).
				ApplyStop::Oversized
				| ApplyStop::ArenaFull {
					empty: true,
				} => {
					self.write_direct_to_l0(&batches[at], oldest_segment(&segments[at + 1..]))?;
					*applied = true;
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
						// Only the batches that were not applied: the rest are spent.
						self.reappend_stale(
							&batches[at..],
							&oversized[at..],
							&mut segments[at..],
							active_tag,
							sync,
							wal_buf,
							wal_ends,
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
	/// returns the index it got to and why it stopped. `estimates` holds what each batch
	/// needs of the memtable. A batch it applied is spent: its keys and values now belong to
	/// the memtable. The batches from the returned index on are untouched.
	///
	/// Synchronous on purpose: no lock guard may live across an await in the
	/// flusher, which runs as a spawned task.
	fn apply_run(
		&self,
		batches: &mut [Batch],
		estimates: &[u64],
		oversized: &[bool],
		segments: &[u64],
		from: usize,
	) -> Result<(usize, ApplyStop)> {
		let active = self.inner.active_memtable.read()?;
		Self::apply_to(&active, batches, estimates, oversized, segments, from)
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
		batches: &mut [Batch],
		estimates: &[u64],
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
		Self::apply_to(&active, batches, estimates, oversized, segments, from)
	}

	/// The loop of `apply_run`, on the memtable the caller holds the read guard of.
	fn apply_to(
		active: &MemTable,
		batches: &mut [Batch],
		estimates: &[u64],
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
			match active.add_owned(&mut batches[at], estimates[at]) {
				Ok(()) => at += 1,
				Err(Error::ArenaFull) => {
					return Ok((
						at,
						ApplyStop::ArenaFull {
							empty: active.is_empty(),
						},
					));
				}
				Err(e) => return Ok((at, ApplyStop::Torn(e))),
			}
		}
		Ok((at, ApplyStop::Done))
	}

	/// Appends again the batches whose record is not in the segment the active memtable is
	/// tagged with (see `encode_stale`), and syncs if the group syncs.
	///
	/// Takes the batches that were not applied yet and no others: the ones before them were
	/// moved into a memtable, and a record built from one of those would be empty.
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
	/// `write_batch_direct_to_l0_sst` marks as captured. `rest_wal_number` is the oldest segment
	/// that holds the record of a batch of the group that comes after this one.
	fn write_direct_to_l0(&self, batch: &Batch, rest_wal_number: u64) -> Result<()> {
		let sealed_wal_number = self.inner.seal_active_wal_segment()?;
		if let Some(ref tm) = self.task_manager {
			tm.wake_up_memtable();
		}

		let table_id = self.inner.level_manifest.read()?.next_table_id();
		self.inner.write_batch_direct_to_l0_sst(
			batch,
			table_id,
			sealed_wal_number,
			rest_wal_number,
		)?;

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
		// Dropped once the mutex is released
		let retained = std::mem::take(&mut *self.overflow.lock());
		drop(retained);
	}
}

/// The oldest of the WAL segments `segments` that hold the records of the batches that are not
/// applied yet, `u64::MAX` if there are none.
fn oldest_segment(segments: &[u64]) -> u64 {
	segments.iter().copied().min().unwrap_or(u64::MAX)
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

/// Turns the values of `batch` into the form that is logged and applied. An inline value
/// becomes an inline `ValueLocation`, a flag byte and a version byte in front of the value, and
/// a value above `vlog_threshold` is appended to the value log and replaced by a pointer to it.
/// A range delete keeps its end key as is.
///
/// In place, so an entry's key and value are not copied. This used to build a second batch
/// entry by entry, which cloned every key and value and encoded each value into a third
/// allocation. `batch.size` stays what `add_record` would have computed for the new values.
fn encode_values(batch: &mut Batch, vlog: Option<&Arc<VLog>>, vlog_threshold: usize) -> Result<()> {
	// What a value adds to `Batch::size`: its length, and the varint that gives it.
	let value_size = |len: usize| varint_len_u64(len as u64) as u64 + len as u64;
	let starting_seq_num = batch.starting_seq_num;
	for (i, entry) in batch.entries.iter_mut().enumerate() {
		if entry.kind == InternalKeyKind::RangeDelete {
			continue;
		}
		let Some(value) = entry.value.as_mut() else {
			continue;
		};
		let size_before = value_size(value.len());
		match vlog {
			Some(vlog) if value.len() > vlog_threshold => {
				let ikey =
					InternalKey::new(entry.key.clone(), starting_seq_num + i as u64, entry.kind);
				let pointer = vlog.append(&ikey.encode(), value)?;
				*value = ValueLocation::with_pointer(pointer).encode();
			}
			_ => {
				// `ValueLocation::with_inline_value(value).encode()`, without the copies.
				let len = value.len();
				value.reserve_exact(2);
				value.resize(len + 2, 0);
				value.copy_within(0..len, 2);
				value[0] = 0;
				value[1] = VALUE_LOCATION_VERSION;
			}
		}
		batch.size = batch.size - size_before + value_size(value.len());
	}
	// The two bytes of every wrapped value count against the limit `add_record` enforces.
	if batch.size > MAX_BATCH_SIZE {
		return Err(Error::BatchTooLarge);
	}
	Ok(())
}

fn bloom_of(keys: &[Key]) -> BloomFilter {
	let mut bloom = BloomFilter::new();
	for k in keys {
		bloom.insert(k);
	}
	bloom
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod encode_values_tests {
	use tempdir::TempDir;

	use super::*;
	use crate::vlog::ValuePointer;
	use crate::{Options, Tree};

	/// What the flusher built before `encode_values`: a second batch, entry by entry, with
	/// every value wrapped in an inline `ValueLocation`.
	fn copying_reference(batch: &Batch) -> Batch {
		let mut processed = Batch::new(batch.starting_seq_num);
		for (_, entry, _seq_num, timestamp) in batch.entries_with_seq_nums().unwrap() {
			let encoded_value = if entry.kind == InternalKeyKind::RangeDelete {
				entry.value.clone()
			} else {
				entry
					.value
					.as_ref()
					.map(|value| ValueLocation::with_inline_value(value.clone()).encode())
			};
			processed.add_record(entry.kind, entry.key.clone(), encoded_value, timestamp).unwrap();
		}
		processed
	}

	#[test]
	fn encoding_values_in_place_logs_the_same_bytes_as_copying_them() {
		for entries in 0..6usize {
			for value_len in [0usize, 1, 2, 7, 8, 9, 125, 126, 127, 128, 129, 1000, 16_381, 16_383]
				.into_iter()
				.chain([16_384, 70_000])
			{
				for kind_offset in 0..5usize {
					let mut batch = Batch::new(0);
					for i in 0..entries {
						let key = vec![i as u8; 1 + i];
						let ts = 100 + i as u64;
						match (i + kind_offset) % 5 {
							0 => batch.set(key, vec![0xAB; value_len], ts).unwrap(),
							1 => batch.delete(key, ts).unwrap(),
							2 => batch
								.add_record(
									InternalKeyKind::RangeDelete,
									key,
									Some(vec![0xFF; value_len.min(3)]),
									ts,
								)
								.unwrap(),
							3 => batch
								.add_record(InternalKeyKind::Set, key, Some(Vec::new()), ts)
								.unwrap(),
							_ => batch
								.add_record(
									InternalKeyKind::SoftDelete,
									key,
									Some(vec![0xCD; value_len]),
									ts,
								)
								.unwrap(),
						}
					}
					batch.set_starting_seq_num(41);

					let expected = copying_reference(&batch);
					let mut expected_bytes = Vec::new();
					expected.encode_into(&mut expected_bytes).unwrap();

					encode_values(&mut batch, None, usize::MAX).unwrap();
					let mut actual_bytes = Vec::new();
					batch.encode_into(&mut actual_bytes).unwrap();

					let what = format!("entries={entries} value_len={value_len} {kind_offset}");
					assert_eq!(actual_bytes, expected_bytes, "{what}");
					assert_eq!(batch.size, expected.size, "{what}");
					assert_eq!(batch.encoded_len_hint(), expected.encoded_len_hint(), "{what}");
					assert_eq!(
						batch.memtable_size_estimate(),
						expected.memtable_size_estimate(),
						"{what}"
					);
					assert!(batch.valueptrs.iter().all(Option::is_none), "{what}");
				}
			}
		}
	}

	/// Large values are appended to the value log and replaced by a pointer, small ones are
	/// wrapped inline, and either way `size` is what `add_record` would have computed.
	#[tokio::test]
	async fn encoding_with_a_value_log_separates_large_values_and_keeps_size_exact() {
		const THRESHOLD: usize = 64;
		let dir = TempDir::new("encode_values").unwrap();
		let tree = Tree::new(Arc::new(Options {
			path: dir.path().to_path_buf(),
			flush_on_close: false,
			enable_vlog: true,
			vlog_value_threshold: THRESHOLD,
			..Default::default()
		}))
		.unwrap();
		let vlog = tree.core.inner.vlog.as_ref().unwrap();

		let lens = [0usize, 1, THRESHOLD - 1, THRESHOLD, THRESHOLD + 1, 200, 5000];
		let mut batch = Batch::new(0);
		for (i, len) in lens.iter().enumerate() {
			let value: Vec<u8> = (0..*len).map(|b| (b + i) as u8).collect();
			batch.set(format!("key{i}").into_bytes(), value, i as u64).unwrap();
		}
		batch.delete(b"deleted".to_vec(), 9).unwrap();
		batch
			.add_record(InternalKeyKind::RangeDelete, b"a".to_vec(), Some(vec![b'z'; 300]), 10)
			.unwrap();
		batch.set_starting_seq_num(1000);
		let original = batch.clone();

		encode_values(&mut batch, Some(vlog), THRESHOLD).unwrap();
		vlog.flush().unwrap();

		for (i, (got, was)) in batch.entries.iter().zip(&original.entries).enumerate() {
			assert_eq!(got.key, was.key);
			assert_eq!(got.kind, was.kind);
			assert_eq!(got.timestamp, was.timestamp);
			match (got.kind, &was.value) {
				(InternalKeyKind::RangeDelete, _) | (_, None) => {
					assert_eq!(got.value, was.value, "entry {i} keeps its value as is")
				}
				(_, Some(value)) => {
					let location = ValueLocation::decode(got.value.as_ref().unwrap()).unwrap();
					if value.len() > THRESHOLD {
						assert!(location.is_value_pointer(), "entry {i} is separated");
						let pointer = ValuePointer::decode(&location.value).unwrap();
						assert_eq!(
							&vlog.get(&pointer).unwrap(),
							value,
							"entry {i} in the value log"
						);
					} else {
						assert!(!location.is_value_pointer(), "entry {i} stays inline");
						assert_eq!(&location.value, value);
					}
				}
			}
		}

		let mut rebuilt = Batch::new(batch.starting_seq_num);
		for entry in &batch.entries {
			rebuilt
				.add_record(entry.kind, entry.key.clone(), entry.value.clone(), entry.timestamp)
				.unwrap();
		}
		assert_eq!(batch.size, rebuilt.size);
		assert_eq!(batch.memtable_size_estimate(), rebuilt.memtable_size_estimate());
		tree.core.is_closed.store(true, Ordering::SeqCst);
	}

	/// The two bytes added to every wrapped value count against the size limit that
	/// `add_record` enforced on the batch the flusher used to build, so a batch that the
	/// wrapping pushes past it fails as it did, and one that it leaves on the limit does not.
	#[test]
	fn a_batch_the_wrapped_values_push_past_the_size_limit_is_refused() {
		// One entry: a set of a 10 byte value under a 3 byte key, which `add_record` counts as
		// kind, key length, key, value length, value and 8 for the timestamp.
		const ENTRY: u64 = 1 + 1 + 3 + 1 + 10 + 8;

		for size in [MAX_BATCH_SIZE - 3, MAX_BATCH_SIZE - 2, MAX_BATCH_SIZE - 1, MAX_BATCH_SIZE] {
			let mut batch = Batch::new(0);
			batch.set(b"key".to_vec(), vec![1u8; 10], 0).unwrap();
			assert_eq!(batch.size, ENTRY);
			batch.size = size;
			let in_place = encode_values(&mut batch, None, usize::MAX);

			// What the copying flusher did: `add_record` of the wrapped value on a new batch
			// that already held the rest of the size.
			let mut processed = Batch::new(0);
			processed.size = size - ENTRY;
			let copied = processed.add_record(
				InternalKeyKind::Set,
				b"key".to_vec(),
				Some(ValueLocation::with_inline_value(vec![1u8; 10]).encode()),
				0,
			);

			assert_eq!(in_place.is_err(), copied.is_err(), "batch of {size} bytes");
			assert_eq!(in_place.is_err(), size + 2 > MAX_BATCH_SIZE, "batch of {size} bytes");
			if let Err(e) = in_place {
				assert!(matches!(e, Error::BatchTooLarge), "got {e:?}");
			}
		}
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod free_retired_tests {
	use std::collections::BTreeMap;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::Arc;

	use parking_lot::Mutex;

	use super::super::commit_ring::{CommitRing, RingEntry};
	use super::{
		free_retired_from,
		retire_due,
		retired_slots_per_step,
		RETIRED_FREE_CHUNK,
		RETIRE_EVERY_ENTRIES,
	};

	fn map(keys: impl IntoIterator<Item = u64>) -> Mutex<BTreeMap<u64, u64>> {
		Mutex::new(keys.into_iter().map(|k| (k, k)).collect())
	}

	fn keys(map: &Mutex<BTreeMap<u64, u64>>) -> Vec<u64> {
		map.lock().keys().copied().collect()
	}

	#[test]
	fn frees_at_most_the_limit_lowest_first() {
		let overflow = map(1..=10);
		let mut freed = Vec::new();
		assert_eq!(free_retired_from(&overflow, 8, 3, &mut freed), (3, true));
		assert_eq!(keys(&overflow), (4..=10).collect::<Vec<_>>());
		assert_eq!(free_retired_from(&overflow, 8, 3, &mut freed), (3, true));
		assert_eq!(keys(&overflow), (7..=10).collect::<Vec<_>>());
		// Two left at or below 8, and the limit is not reached
		assert_eq!(free_retired_from(&overflow, 8, 3, &mut freed), (2, false));
		assert_eq!(keys(&overflow), vec![9, 10]);
		assert!(freed.is_empty(), "the buffer is handed back empty");
	}

	#[test]
	fn never_frees_above_taken() {
		let overflow = map([5, 6, 7, 20, 21]);
		let mut freed = Vec::new();
		assert_eq!(free_retired_from(&overflow, 4, 100, &mut freed), (0, false));
		assert_eq!(free_retired_from(&overflow, 7, 100, &mut freed), (3, false));
		assert_eq!(keys(&overflow), vec![20, 21]);
		assert_eq!(free_retired_from(&overflow, 19, usize::MAX, &mut freed), (0, false));
		assert_eq!(keys(&overflow), vec![20, 21]);
		// The watermark itself is retired
		assert_eq!(free_retired_from(&overflow, 20, 100, &mut freed), (1, false));
		assert_eq!(keys(&overflow), vec![21]);
	}

	#[test]
	fn reports_more_exactly_when_an_entry_at_or_below_taken_is_left() {
		let overflow = map([1, 2, 3, 9]);
		let mut freed = Vec::new();
		// Exactly the retired entries: nothing is left, though the limit is reached
		assert_eq!(free_retired_from(&overflow, 3, 3, &mut freed), (3, false));
		// An entry above the watermark is not "more"
		assert_eq!(free_retired_from(&overflow, 3, 3, &mut freed), (0, false));
		assert_eq!(free_retired_from(&overflow, 9, 0, &mut freed), (0, true));
		assert_eq!(free_retired_from(&overflow, 9, 1, &mut freed), (1, false));
		assert_eq!(
			free_retired_from(&Mutex::new(BTreeMap::<u64, u64>::new()), 9, 1, &mut freed),
			(0, false)
		);
	}

	#[test]
	fn repeated_calls_free_exactly_the_entries_at_or_below_taken() {
		let mut rng = fastrand::Rng::with_seed(0x5eed);
		for _ in 0..500 {
			let keys_in: Vec<u64> = (0..rng.usize(0..200)).map(|_| rng.u64(1..400)).collect();
			let overflow = map(keys_in.iter().copied());
			let (taken, limit) = (rng.u64(0..420), rng.usize(1..40));
			let before = keys(&overflow);
			let mut freed = Vec::new();
			let mut calls = 0;
			let mut total = 0;
			loop {
				let (count, more) = free_retired_from(&overflow, taken, limit, &mut freed);
				assert!(count <= limit, "freed {count} with a limit of {limit}");
				total += count;
				calls += 1;
				if !more {
					break;
				}
				assert_eq!(count, limit, "more must mean the limit was reached");
			}
			let retired = before.iter().filter(|k| **k <= taken).count();
			assert_eq!(total, retired);
			assert_eq!(calls, retired.div_ceil(limit).max(1));
			assert_eq!(
				keys(&overflow),
				before.into_iter().filter(|k| *k > taken).collect::<Vec<_>>()
			);
		}
	}

	/// The ring slots a retire has passed and the flusher has not released yet, with the ring at
	/// the size production uses and driven the way the flusher drives it: groups of a fixed size,
	/// a retire every `RETIRE_EVERY_GROUPS` of them or `RETIRE_EVERY_ENTRIES` entries, and one
	/// release step after each group. They must not pile up, whatever the size of the group: a
	/// step of a fixed number of slots falls behind from about `RETIRED_FREE_CHUNK` entries a
	/// group, and leaves retired entries in most of the ring for as long as the load lasts.
	#[test]
	fn retired_slots_are_released_as_fast_as_groups_retire_them() {
		struct Entry;
		impl RingEntry for Entry {
			fn is_complete(&self) -> bool {
				true
			}
		}

		const CAPACITY: u64 = 65536;
		for group in [1u64, 100, 256, 257, 300, 512, 1024, 4096] {
			let ring = CommitRing::<Entry>::new(CAPACITY as usize, 1);
			let (mut groups, mut entries, mut sampled) = (0u32, 0u64, 0u64);
			let mut worst = 0;
			while ring.published() < 3 * CAPACITY {
				for _ in 0..group {
					let seq = ring.claim();
					ring.publish(seq, Arc::new(Entry));
				}
				ring.advance_published();
				ring.advance_completed();
				groups += 1;
				entries += group;
				if retire_due(groups, entries) {
					(groups, entries) = (0, 0);
					ring.raise_taken(ring.completed());
				}
				ring.release_retired(retired_slots_per_step(group));
				// Counting the slots costs a lap of the ring, so only now and then
				if ring.published() >= sampled + 4096 {
					sampled = ring.published();
					let unretired = (ring.published() - ring.taken()).min(CAPACITY) as usize;
					worst = worst.max(ring.occupied().saturating_sub(unretired));
				}
			}
			let bound = RETIRE_EVERY_ENTRIES + group + RETIRED_FREE_CHUNK as u64;
			assert!(
				worst as u64 <= bound,
				"with groups of {group}, {worst} slots were retired and not yet released (more than {bound})"
			);
		}
	}

	/// An entry is dropped when the overflow map is no longer locked, so a drop that takes a while
	/// never holds up a committer evicting into the map or validating against it.
	#[test]
	fn drops_the_entries_after_the_mutex_is_released() {
		struct Probe {
			map: Arc<Mutex<BTreeMap<u64, Probe>>>,
			locked_when_dropped: Arc<AtomicUsize>,
		}
		impl Drop for Probe {
			fn drop(&mut self) {
				if self.map.try_lock().is_none() {
					self.locked_when_dropped.fetch_add(1, Ordering::SeqCst);
				}
			}
		}

		let overflow = Arc::new(Mutex::new(BTreeMap::new()));
		let locked_when_dropped = Arc::new(AtomicUsize::new(0));
		for seq in 1..=8u64 {
			let probe = Probe {
				map: Arc::clone(&overflow),
				locked_when_dropped: Arc::clone(&locked_when_dropped),
			};
			overflow.lock().insert(seq, probe);
		}
		let mut freed = Vec::new();
		assert_eq!(free_retired_from(&overflow, 6, 4, &mut freed), (4, true));
		assert_eq!(free_retired_from(&overflow, 6, 4, &mut freed), (2, false));
		assert_eq!(overflow.lock().len(), 2);
		assert_eq!(
			locked_when_dropped.load(Ordering::SeqCst),
			0,
			"entries were dropped while the overflow map was locked"
		);
		// The two left are dropped with the map, which breaks the cycle through `Probe::map`
		let rest = std::mem::take(&mut *overflow.lock());
		drop(rest);
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod group_budget_tests {
	use super::*;

	#[test]
	fn a_group_always_takes_its_first_entry_and_then_only_what_fits_the_budget() {
		// An entry over the budget is a group of its own: it is taken by an empty group, and
		// nothing is taken after it.
		assert!(group_has_room(0, 0, MAX_GROUP_BYTES + 1));
		assert!(group_has_room(0, 0, usize::MAX));
		assert!(!group_has_room(1, MAX_GROUP_BYTES + 1, 1));
		// Within the budget, up to and including the byte that fills it.
		assert!(group_has_room(1, 100, MAX_GROUP_BYTES - 100));
		assert!(!group_has_room(1, 100, MAX_GROUP_BYTES - 100 + 1));
		assert!(group_has_room(3, MAX_GROUP_BYTES - 1, 1));
		assert!(!group_has_room(3, MAX_GROUP_BYTES, 1));
		// A size that overflows the sum is over the budget, not wrapped back under it.
		assert!(!group_has_room(1, 10, usize::MAX));
		assert!(!group_has_room(1, usize::MAX, usize::MAX));
	}

	#[test]
	fn a_wal_buffer_the_allocator_cannot_give_is_an_error_not_an_abort() {
		let mut buf = Vec::new();
		let err = try_reserve_wal(&mut buf, usize::MAX).expect_err("no allocator has this");
		match err {
			Error::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::OutOfMemory),
			e => panic!("expected an I/O error, got {e:?}"),
		}
		assert_eq!(buf.capacity(), 0, "nothing was allocated");

		try_reserve_wal(&mut buf, 1000).unwrap();
		assert_eq!(buf.capacity(), 1000, "exactly what was asked for");
		let ptr = buf.as_ptr();
		try_reserve_wal(&mut buf, 600).unwrap();
		assert_eq!(buf.as_ptr(), ptr, "a buffer that is big enough is not reallocated");
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod scratch_tests {
	use tempdir::TempDir;

	use super::*;
	use crate::{Options, Tree};

	#[test]
	fn recycle_empties_a_buffer_and_gives_back_one_that_grew_too_big() {
		let mut kept = Vec::with_capacity(8);
		kept.extend(0..8u64);
		recycle(&mut kept, 8);
		assert!(kept.is_empty());
		assert!(kept.capacity() >= 8, "a buffer at the limit keeps its allocation");

		let mut grown = Vec::with_capacity(9);
		grown.extend(0..9u64);
		recycle(&mut grown, 8);
		assert!(grown.is_empty());
		assert_eq!(grown.capacity(), 0, "a buffer past the limit is dropped");
	}

	#[test]
	fn a_scratch_that_a_burst_grew_is_emptied_and_shrunk() {
		let mut scratch = FlushScratch::default();
		scratch.batches.extend((0..4).map(|_| Batch::new(1)));
		scratch.estimates.extend(0..4);
		scratch.oversized.extend([false; 4]);
		scratch.segments.extend(0..4);
		scratch.wal_buf.extend([0u8; 16]);
		scratch.wal_ends.extend(0..4);
		scratch.recycle();
		assert!(scratch.batches.is_empty() && scratch.estimates.is_empty());
		assert!(scratch.oversized.is_empty() && scratch.segments.is_empty());
		assert!(scratch.wal_buf.is_empty() && scratch.wal_ends.is_empty());
		assert!(scratch.group.is_empty() && scratch.entries.is_empty());
		assert!(scratch.waiters.is_empty());
		assert!(scratch.wal_buf.capacity() >= 16, "ordinary buffers are kept");

		scratch.wal_buf.resize(MAX_SCRATCH_BYTES + 1, 0);
		scratch.estimates.resize(MAX_SCRATCH_ELEMENTS + 1, 0);
		scratch.recycle();
		assert_eq!(scratch.wal_buf.capacity(), 0, "a WAL buffer past the cap is dropped");
		assert_eq!(scratch.estimates.capacity(), 0, "a vector past its limit is dropped");
	}

	#[tokio::test]
	async fn merging_permits_gives_every_one_back_with_the_last_to_drop() {
		let semaphore = Arc::new(Semaphore::new(5));
		let mut merged = None;
		merge_permit(&mut merged, None);
		assert!(merged.is_none(), "no permit, nothing merged");
		for _ in 0..3 {
			let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
			merge_permit(&mut merged, Some(permit));
		}
		merge_permit(&mut merged, None);
		assert_eq!(semaphore.available_permits(), 2, "three permits are held by one");
		drop(merged);
		assert_eq!(semaphore.available_permits(), 5, "one drop gives all three back");
	}

	/// A group at the byte budget keeps its WAL buffer for the next group: `recycle` does not
	/// drop it, and a group that fits it does not reallocate it.
	#[tokio::test]
	async fn a_group_at_the_byte_budget_reuses_its_wal_buffer() {
		let dir = TempDir::new("scratch").unwrap();
		let tree = Tree::new(Arc::new(Options {
			path: dir.path().to_path_buf(),
			flush_on_close: false,
			..Default::default()
		}))
		.unwrap();
		let pipeline = &tree.core.commit_pipeline;
		let mut scratch = FlushScratch::default();

		// Three batches that fill the budget between them, as the flusher counts them.
		let group = |round: u8, value_len: usize| -> Vec<Batch> {
			(0..3u8)
				.map(|i| {
					let mut batch = Batch::new(u64::from(round) * 10 + u64::from(i));
					let key = format!("key{round}_{i}").into_bytes();
					batch
						.add_record(InternalKeyKind::Set, key, Some(vec![round; value_len]), 0)
						.unwrap();
					batch
				})
				.collect()
		};
		let hints = |batches: &[Batch]| batches.iter().map(Batch::encoded_len_hint).sum::<usize>();

		let mut smaller = group(1, MAX_GROUP_BYTES / 3 - 200);
		let mut larger = group(2, MAX_GROUP_BYTES / 3 - 100);
		assert!(hints(&smaller) < hints(&larger) && hints(&larger) <= MAX_GROUP_BYTES);
		pipeline.flush_group_with(&mut smaller, false, &mut scratch).await.unwrap();
		scratch.recycle();
		assert!(scratch.wal_buf.capacity() >= hints(&smaller), "kept: a group at the budget");

		// A bigger group grows the buffer once, to what it needs and not to a doubling of it.
		pipeline.flush_group_with(&mut larger, false, &mut scratch).await.unwrap();
		scratch.recycle();
		let capacity = scratch.wal_buf.capacity();
		assert!(capacity >= hints(&larger), "kept: the largest group so far");
		assert!(capacity <= hints(&larger) + 3 * 2 + 64, "grown to need, not doubled: {capacity}");
		assert!(capacity <= MAX_SCRATCH_BYTES);

		let ptr = scratch.wal_buf.as_ptr();
		for round in 3..6 {
			let mut again = group(round, MAX_GROUP_BYTES / 3 - 100);
			pipeline.flush_group_with(&mut again, false, &mut scratch).await.unwrap();
			scratch.recycle();
			assert_eq!(scratch.wal_buf.as_ptr(), ptr, "round {round} reallocated the buffer");
			assert_eq!(scratch.wal_buf.capacity(), capacity);
		}
		tree.core.is_closed.store(true, Ordering::SeqCst);
	}

	/// A group's reservation reaches the allocator through `try_reserve_wal`: a size no
	/// allocator can give is an error from `reserve_wal_buf`, where `Vec::reserve` panics, and the
	/// test failpoint refuses from its threshold up.
	#[tokio::test]
	async fn the_wal_buffer_reservation_of_a_group_fails_instead_of_aborting() {
		let dir = TempDir::new("scratch").unwrap();
		let tree = Tree::new(Arc::new(Options {
			path: dir.path().to_path_buf(),
			flush_on_close: false,
			..Default::default()
		}))
		.unwrap();
		let pipeline = &tree.core.commit_pipeline;
		let mut buf = Vec::new();
		// Past what any `Vec<u8>` can hold, and not `usize::MAX`, which lifts the failpoint.
		assert!(pipeline.reserve_wal_buf(&mut buf, isize::MAX as usize + 1).is_err());
		assert_eq!(buf.capacity(), 0, "nothing was allocated");

		pipeline.set_wal_reserve_fails_from(100);
		assert!(pipeline.reserve_wal_buf(&mut buf, 100).is_err());
		pipeline.reserve_wal_buf(&mut buf, 99).unwrap();
		assert_eq!(buf.capacity(), 99);
		pipeline.set_wal_reserve_fails_from(usize::MAX);
		let mut other = Vec::new();
		pipeline.reserve_wal_buf(&mut other, 100).unwrap();
		tree.core.is_closed.store(true, Ordering::SeqCst);
	}

	/// A batch of the smallest entries counts for at most `MAX_GROUP_BYTES` when the flusher
	/// gathers it, and for two bytes an entry more once its values are wrapped: a seventh over
	/// the budget, far more than the 64 KiB that two bytes per commit would add. The cap on the
	/// kept buffer has to cover that.
	#[test]
	fn the_cap_on_the_wal_buffer_covers_a_group_of_the_smallest_entries_at_the_budget() {
		let mut batch = Batch::new(0);
		// 14 bytes an entry: its kind, a one byte key length and value length, the timestamp and
		// the slack that the hint adds.
		while batch.encoded_len_hint() + 14 <= MAX_GROUP_BYTES {
			batch.add_record(InternalKeyKind::Set, Vec::new(), Some(Vec::new()), 0).unwrap();
		}
		assert!(batch.encoded_len_hint() <= MAX_GROUP_BYTES, "the gather lets it into a group");
		encode_values(&mut batch, None, usize::MAX).unwrap();
		let needs = batch.encoded_len_hint();
		assert!(needs > MAX_GROUP_BYTES + (64 << 10), "wrapping adds more than 64 KiB: {needs}");
		assert!(needs <= MAX_GROUP_BYTES + MAX_GROUP_BYTES / 7 + 14, "a seventh over: {needs}");
		assert!(needs <= MAX_SCRATCH_BYTES, "a group at the budget is kept: {needs}");
	}

	/// `apply_run` moves a batch into the memtable on the strength of the size it is given. If
	/// that size was wrong and the arena runs out after a successful reservation, the batch is
	/// partly moved, and applying it again (which `ArenaFull` would mean) would insert empty
	/// keys. It has to stop the group as `Torn`, a hard error that is neither retried nor counted
	/// as applied, whatever the arena says.
	#[tokio::test]
	async fn apply_run_fails_hard_instead_of_retrying_when_a_size_was_wrong() {
		let dir = TempDir::new("scratch").unwrap();
		let tree = Tree::new(Arc::new(Options {
			path: dir.path().to_path_buf(),
			flush_on_close: false,
			max_memtable_size: 4096,
			..Default::default()
		}))
		.unwrap();
		let pipeline = &tree.core.commit_pipeline;
		let tag = tree.core.inner.active_memtable.read().unwrap().get_wal_number();

		let mut batch = Batch::new(1);
		for i in 0..400u32 {
			let value = ValueLocation::with_inline_value(b"value".to_vec()).encode();
			batch
				.add_record(InternalKeyKind::Set, format!("key{i:05}").into_bytes(), Some(value), 0)
				.unwrap();
		}
		let mut batches = [batch];
		let outcome = pipeline.apply_run(&mut batches, &[100], &[false], &[tag], 0);
		match outcome {
			Ok((at, ApplyStop::Torn(Error::Other(message)))) => {
				assert!(message.contains("drift"), "{message}");
				assert_eq!(at, 0, "the batch that was partly moved is not counted as applied");
			}
			Ok((at, _)) => panic!("applied or retried, stopped at {at}"),
			Err(e) => panic!("expected a torn stop, got {e:?}"),
		}
		tree.core.is_closed.store(true, Ordering::SeqCst);
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod permit_merge_tests {
	use std::sync::atomic::AtomicUsize;
	use std::time::{Duration, Instant};

	use tempdir::TempDir;

	use super::*;
	use crate::{Mode, Options, Tree};

	/// Polls `done` until it holds, and fails the test if it never does: a permit that is not
	/// given back leaves a commit blocked for good, which must not hang the suite.
	async fn until(what: &str, done: impl Fn() -> bool) {
		let deadline = Instant::now() + Duration::from_secs(60);
		while !done() {
			assert!(Instant::now() < deadline, "timed out waiting for {what}");
			tokio::time::sleep(Duration::from_millis(2)).await;
		}
	}

	/// Holds the flusher after the WAL write of its first group, so that every commit behind
	/// it piles up: half the ring's worth of them take an admission permit and the rest block
	/// on the semaphore. The next group is then as big as admission allows, and the flusher
	/// gives back all of its permits at once. Every commit, those that were blocked included,
	/// must complete, and every permit must be back in the semaphore.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_full_group_gives_back_every_permit_and_the_commits_behind_it_proceed() {
		let dir = TempDir::new("permit_merge").unwrap();
		let tree = Arc::new(
			Tree::new(Arc::new(Options {
				path: dir.path().to_path_buf(),
				..Default::default()
			}))
			.unwrap(),
		);
		let pipeline = &tree.core.commit_pipeline;
		let permits = DEFAULT_COMMIT_RING_CAPACITY / 2;

		let held = Arc::new(AtomicBool::new(false));
		let largest_group = Arc::new(AtomicUsize::new(0));
		let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
		let release_rx = std::sync::Mutex::new(release_rx);
		let (hook_held, hook_largest) = (Arc::clone(&held), Arc::clone(&largest_group));
		pipeline.set_hook(Some(Arc::new(move |point| {
			if let PipelineHook::AfterWalSync {
				batches,
			} = point
			{
				hook_largest.fetch_max(batches, Ordering::SeqCst);
				if !hook_held.swap(true, Ordering::SeqCst) {
					release_rx.lock().unwrap().recv().unwrap();
				}
			}
		})));

		let commits = permits + 200;
		let handles: Vec<_> = (0..commits)
			.map(|i| {
				let tree = Arc::clone(&tree);
				tokio::spawn(async move {
					let mut txn = tree.begin().unwrap();
					txn.set(format!("key{i:05}").into_bytes(), b"v".to_vec()).unwrap();
					txn.commit().await
				})
			})
			.collect();

		until("the flusher to hold its first group", || held.load(Ordering::SeqCst)).await;
		until("admission to run out", || pipeline.admission.available_permits() == 0).await;
		// Let the commits that took the last permits be accepted into the ring.
		tokio::time::sleep(Duration::from_millis(50)).await;
		release_tx.send(()).unwrap();

		for handle in handles {
			let commit = tokio::time::timeout(Duration::from_secs(60), handle)
				.await
				.expect("a commit stayed blocked on admission: a permit was not given back");
			commit.unwrap().unwrap();
		}
		until("every permit to come back", || pipeline.admission.available_permits() == permits)
			.await;
		assert!(
			largest_group.load(Ordering::SeqCst) >= permits / 2,
			"the pile-up was flushed as one big group, not {} batches at most",
			largest_group.load(Ordering::SeqCst)
		);

		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for i in 0..commits {
			let key = format!("key{i:05}");
			assert_eq!(txn.get(key.as_bytes()).unwrap().as_deref(), Some(&b"v"[..]), "{key}");
		}
		drop(txn);
		pipeline.set_hook(None);
		tree.close().await.unwrap();
	}
}
