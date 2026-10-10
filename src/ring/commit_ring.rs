//! A lock-free, fixed-size, power-of-two circular ring buffer for the commit
//! pipeline.
//!
//! Ported from SurrealMX (`src/ring.rs`), with one addition:
//! [`CommitRing::publish_with`] hands a still-held previous occupant to the
//! caller, under the slot's write lock, before overwriting it.
//!
//! Every committing transaction claims the next sequence number with a
//! single `fetch_add` and publishes its entry into slot `seq & mask`. Three
//! contiguous watermarks are derived from the slots:
//!
//! - the published prefix: every sequence at or below it has been written;
//! - the completed prefix: every entry at or below it has either published its merge version or
//!   aborted (see [`RingEntry::is_complete`]);
//! - the retired prefix: no live transaction's conflict window reaches it.
//!
//! A slot is reused one lap later, by `seq + capacity`. The publisher of
//! the later sequence waits until the slot's previous occupant is both
//! published and complete before overwriting it. That wait is bounded by
//! the duration of one in-flight commit, never by a reader, and it is what
//! keeps the watermarks sound across laps: an overwritten slot always held
//! a published, complete entry, so neither prefix can wedge behind a slot
//! that was overwritten before the prefix reached it, and the completed
//! prefix can never pass an entry that is still in flight.
//!
//! A slot's sequence number is only written while holding the slot's write
//! lock, so a reader holding the read lock always sees the sequence number
//! that matches the entry. A reader that finds a later occupant knows the
//! entry it wanted is gone, and must treat it conservatively.
//!
//! Once the retired prefix passes an entry, its slot releases the entry but
//! keeps its sequence number, so retired commits stop pinning memory while
//! waiting a lap to be overwritten. The release is not part of retiring: a
//! large retired range is released a bounded number of slots at a time
//! ([`CommitRing::release_retired`]), and until then the entry is still
//! readable, though nothing reads it. The retired prefix never passes the
//! completed prefix, so a released entry was complete, and a slot that
//! holds its own sequence number without an entry reads as gone.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::sync::{backoff, AtomicU64, CachePadded, RwLock};

/// Default capacity of the commit ring buffer (must be a power of two).
/// Test and Miri builds use a far smaller ring, so that ordinary tests lap it
/// and exercise the overflow path for long-lived conflict windows.
pub(crate) const DEFAULT_COMMIT_RING_CAPACITY: usize = if cfg!(any(test, miri)) {
	1024
} else {
	65536
};

/// The sequence number held by a slot that has never been published.
/// Sequence numbers handed out by a ring always start above it.
const SLOT_EMPTY: u64 = 0;

/// An entry stored in the commit ring.
pub(crate) trait RingEntry {
	/// Whether the owning transaction has finished with this entry, having
	/// either published its merge version or aborted. Must be monotonic:
	/// once it returns `true` it must never return `false` again.
	fn is_complete(&self) -> bool;
}

/// The outcome of reading a ring slot for a specific sequence number.
pub(crate) enum SlotRead<R> {
	/// The sequence has been claimed, but its entry is not published yet.
	Pending,
	/// The entry was published and complete, and has since been overwritten
	/// by a later lap or released after retirement, so its contents are no
	/// longer available.
	Gone,
	/// The entry published at this sequence.
	Ready(R),
}

/// A single pre-allocated slot in the commit ring.
struct Slot<T> {
	/// The sequence number of the current occupant, or [`SLOT_EMPTY`].
	/// Only written while holding the `entry` write lock, after the entry
	/// itself, so it is stable while the read lock is held and a lock-free
	/// load that observes a sequence number is always backed by a
	/// published entry.
	seq: AtomicU64,
	/// The current occupant's entry.
	entry: RwLock<Option<Arc<T>>>,
}

/// A fixed-size power-of-two lock-free ring buffer for OCC commits.
pub(crate) struct CommitRing<T> {
	/// The pre-allocated circular array of slots.
	slots: Box<[Slot<T>]>,
	/// Bitmask for fast circular indexing: `seq & mask`.
	mask: u64,
	/// The number of slots, and so the distance between two sequence
	/// numbers sharing a slot.
	capacity: u64,
	/// The first sequence number handed out by this ring.
	start: u64,
	/// The next sequence number to hand out to a committing transaction.
	next: CachePadded<AtomicU64>,
	/// The contiguous published prefix bound.
	published: CachePadded<AtomicU64>,
	/// The contiguous completed prefix bound.
	completed: CachePadded<AtomicU64>,
	/// The highest sequence number that has been retired: no live or future
	/// transaction's conflict window reaches at or below it.
	taken: CachePadded<AtomicU64>,
	/// The highest sequence number whose slot `release_retired` has visited,
	/// never above `taken`.
	released: CachePadded<AtomicU64>,
}

impl<T: RingEntry> CommitRing<T> {
	/// Creates a new commit ring buffer with a capacity rounded up to the next
	/// power of two, handing out sequence numbers from `start_seq` (at least
	/// one, since zero marks an empty slot).
	pub(crate) fn new(capacity: usize, start_seq: u64) -> Self {
		let capacity = capacity.max(1).next_power_of_two();
		let start = start_seq.max(SLOT_EMPTY + 1);
		let slots = (0..capacity)
			.map(|_| Slot {
				seq: AtomicU64::new(SLOT_EMPTY),
				entry: RwLock::new(None),
			})
			.collect();
		Self {
			slots,
			mask: capacity as u64 - 1,
			capacity: capacity as u64,
			start,
			next: CachePadded::new(AtomicU64::new(start)),
			published: CachePadded::new(AtomicU64::new(start - 1)),
			completed: CachePadded::new(AtomicU64::new(start - 1)),
			taken: CachePadded::new(AtomicU64::new(start - 1)),
			released: CachePadded::new(AtomicU64::new(start - 1)),
		}
	}

	/// Atomically claims the next sequential slot in O(1).
	#[inline(always)]
	pub(crate) fn claim(&self) -> u64 {
		self.next.fetch_add(1, Ordering::Relaxed)
	}

	/// Accesses the slot for the given sequence number in O(1).
	#[inline(always)]
	fn slot(&self, seq: u64) -> &Slot<T> {
		let idx = usize::try_from(seq & self.mask).unwrap_or(0);
		&self.slots[idx]
	}

	/// Publishes the entry for a claimed sequence number.
	///
	/// Waits while the slot's previous occupant, one lap earlier, is still
	/// unpublished or incomplete: see the module documentation for why
	/// overwriting it then would be unsound.
	#[cfg(test)]
	pub(crate) fn publish(&self, seq: u64, entry: Arc<T>) {
		self.publish_with(seq, entry, |_, _| {});
	}

	/// As [`publish`](Self::publish), calling `evict(prev_seq, prev_entry)`
	/// with the previous occupant, if the slot still holds one, while the
	/// slot's write lock is held and before the occupant is replaced. A reader
	/// that finds the slot overwritten can therefore rely on anything `evict`
	/// recorded.
	pub(crate) fn publish_with(&self, seq: u64, entry: Arc<T>, evict: impl FnOnce(u64, &Arc<T>)) {
		let mut evict = Some(evict);
		let slot = self.slot(seq);
		// The sequence number this slot held one lap earlier, if any
		let previous = seq.checked_sub(self.capacity).filter(|p| *p >= self.start);
		let mut spins = 0;
		loop {
			let mut guard = slot.entry.write();
			let free = match previous {
				// First lap: nothing has ever been stored in this slot
				None => true,
				Some(p) => {
					slot.seq.load(Ordering::Relaxed) == p
						&& guard.as_ref().is_none_or(|e| e.is_complete())
				}
			};
			if free {
				if let (Some(p), Some(prev), Some(evict)) = (previous, guard.as_ref(), evict.take())
				{
					evict(p, prev);
				}
				let replaced = guard.replace(entry);
				slot.seq.store(seq, Ordering::Release);
				drop(guard);
				// Release the previous occupant outside the lock
				drop(replaced);
				return;
			}
			drop(guard);
			backoff(spins);
			spins += 1;
		}
	}

	/// Reads the entry published at `seq`.
	pub(crate) fn get(&self, seq: u64) -> SlotRead<Arc<T>> {
		self.read(seq, Arc::clone)
	}

	/// Inspects the entry published at `seq` under the slot's read lock.
	pub(crate) fn read<R>(&self, seq: u64, f: impl FnOnce(&Arc<T>) -> R) -> SlotRead<R> {
		let slot = self.slot(seq);
		// Lock-free fast path while the sequence is unpublished
		if slot.seq.load(Ordering::Acquire) < seq {
			return SlotRead::Pending;
		}
		let guard = slot.entry.read();
		// Stable under the read lock, so it describes `guard`
		let current = slot.seq.load(Ordering::Relaxed);
		match (current.cmp(&seq), guard.as_ref()) {
			(std::cmp::Ordering::Equal, Some(entry)) => SlotRead::Ready(f(entry)),
			// Released after retirement, or overwritten by a later lap
			(std::cmp::Ordering::Equal, None) | (std::cmp::Ordering::Greater, _) => SlotRead::Gone,
			(std::cmp::Ordering::Less, _) => SlotRead::Pending,
		}
	}

	/// Advances the contiguous published prefix as far as currently
	/// possible.
	pub(crate) fn advance_published(&self) {
		// Every sequence below this bound has been claimed
		let claimed = self.next.load(Ordering::Acquire);
		let mut cur = self.published.load(Ordering::Acquire);
		loop {
			let mut target = cur;
			// A slot only moves on to a later lap once its occupant was
			// published, so a later sequence number means ours was too
			while target + 1 < claimed && self.slot(target + 1).seq.load(Ordering::Acquire) > target
			{
				target += 1;
			}
			if target <= cur {
				return;
			}
			match self.published.compare_exchange_weak(
				cur,
				target,
				Ordering::SeqCst,
				Ordering::Acquire,
			) {
				Ok(_) => return,
				Err(actual) if actual >= target => return,
				Err(actual) => cur = actual,
			}
		}
	}

	/// Advances the contiguous completed prefix as far as currently
	/// possible. Never passes the published prefix.
	pub(crate) fn advance_completed(&self) {
		let published = self.published.load(Ordering::Acquire);
		let mut cur = self.completed.load(Ordering::Acquire);
		loop {
			let mut target = cur;
			while target < published {
				let complete = match self.read(target + 1, |e| e.is_complete()) {
					SlotRead::Ready(complete) => complete,
					// Only a complete occupant is ever overwritten or
					// released. A released slot is only reached from a
					// stale view of this prefix, which already passed it.
					SlotRead::Gone => true,
					SlotRead::Pending => false,
				};
				if !complete {
					break;
				}
				target += 1;
			}
			if target <= cur {
				return;
			}
			match self.completed.compare_exchange_weak(
				cur,
				target,
				Ordering::SeqCst,
				Ordering::Acquire,
			) {
				Ok(_) => return,
				Err(actual) if actual >= target => return,
				Err(actual) => cur = actual,
			}
		}
	}

	/// Raises the retired watermark `taken`. It releases nothing, however far it
	/// moves: [`release_retired`](Self::release_retired) does.
	///
	/// The caller must guarantee that no live or future conflict window
	/// reaches at or below `new_taken`. The watermark is also clamped to the
	/// completed prefix, which keeps released slots out of the completed
	/// prefix's own scan and makes every released entry a complete one.
	pub(crate) fn raise_taken(&self, new_taken: u64) {
		let new_taken = new_taken.min(self.completed());
		self.taken.fetch_max(new_taken, Ordering::SeqCst);
	}

	/// Releases the entries of up to `limit` slots that the retired watermark
	/// has passed, lowest first, so that retired slots stop pinning memory,
	/// and reports whether more are left.
	pub(crate) fn release_retired(&self, limit: u64) -> bool {
		let taken = self.taken();
		// Only the most recent lap of sequences can still hold an entry
		let from = self.released.load(Ordering::SeqCst).max(taken.saturating_sub(self.capacity));
		let to = taken.min(from.saturating_add(limit));
		for seq in (from + 1)..=to {
			self.release(seq);
		}
		// Releasing a slot twice is harmless, so concurrent callers need no more than this
		self.released.fetch_max(to, Ordering::SeqCst);
		to < taken
	}

	/// Raises the retired watermark and releases every slot it passes.
	#[cfg(test)]
	pub(crate) fn advance_taken(&self, new_taken: u64) {
		self.raise_taken(new_taken);
		self.release_retired(u64::MAX);
	}

	/// Drops the entry of a retired sequence, keeping its sequence number.
	fn release(&self, seq: u64) {
		let slot = self.slot(seq);
		let mut guard = slot.entry.write();
		// A later lap may already own the slot
		if slot.seq.load(Ordering::Relaxed) == seq {
			let released = guard.take();
			drop(guard);
			drop(released);
		}
	}

	/// The contiguous published prefix bound.
	#[cfg(test)]
	#[inline]
	pub(crate) fn published(&self) -> u64 {
		self.published.load(Ordering::SeqCst)
	}

	/// The contiguous completed prefix bound: every entry at or below it
	/// has published its merge version or aborted.
	#[inline]
	pub(crate) fn completed(&self) -> u64 {
		self.completed.load(Ordering::SeqCst)
	}

	/// The retired watermark.
	#[inline]
	pub(crate) fn taken(&self) -> u64 {
		self.taken.load(Ordering::SeqCst)
	}

	/// The number of slots in the ring.
	#[cfg(test)]
	pub(crate) const fn capacity(&self) -> u64 {
		self.capacity
	}

	/// The number of slots currently holding an entry.
	#[cfg(test)]
	pub(crate) fn occupied(&self) -> usize {
		self.slots.iter().filter(|s| s.entry.read().is_some()).count()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::AtomicBool;
	use std::thread;
	use std::time::Duration;

	use super::*;

	struct Entry {
		seq: u64,
		done: AtomicBool,
	}

	impl RingEntry for Entry {
		fn is_complete(&self) -> bool {
			self.done.load(Ordering::SeqCst)
		}
	}

	fn entry(seq: u64, done: bool) -> Arc<Entry> {
		Arc::new(Entry {
			seq,
			done: AtomicBool::new(done),
		})
	}

	fn seq_of(read: SlotRead<Arc<Entry>>) -> Option<u64> {
		match read {
			SlotRead::Ready(e) => Some(e.seq),
			_ => None,
		}
	}

	const fn is_pending(read: &SlotRead<Arc<Entry>>) -> bool {
		matches!(read, SlotRead::Pending)
	}

	const fn is_gone(read: &SlotRead<Arc<Entry>>) -> bool {
		matches!(read, SlotRead::Gone)
	}

	fn advance(ring: &CommitRing<Entry>) {
		ring.advance_published();
		ring.advance_completed();
	}

	/// Wait long enough for a blocked publisher to have run if it could.
	fn settle() {
		thread::sleep(Duration::from_millis(if cfg!(miri) {
			1
		} else {
			50
		}));
	}

	#[test]
	fn capacity_rounds_up_to_a_power_of_two() {
		assert_eq!(CommitRing::<Entry>::new(0, 1).capacity(), 1);
		assert_eq!(CommitRing::<Entry>::new(1, 1).capacity(), 1);
		assert_eq!(CommitRing::<Entry>::new(3, 1).capacity(), 4);
		assert_eq!(CommitRing::<Entry>::new(64, 1).capacity(), 64);
		assert_eq!(CommitRing::<Entry>::new(65, 1).capacity(), 128);
	}

	#[test]
	fn claims_are_dense_from_the_start_sequence() {
		let ring = CommitRing::<Entry>::new(8, 10);
		assert_eq!(ring.claim(), 10);
		assert_eq!(ring.claim(), 11);
		assert_eq!(ring.claim(), 12);
		assert_eq!(ring.published(), 9);
		assert_eq!(ring.completed(), 9);
		assert_eq!(ring.taken(), 9);
	}

	#[test]
	fn start_sequence_zero_is_clamped_above_the_empty_marker() {
		// Sequence zero would read as published before it was
		let ring = CommitRing::<Entry>::new(8, 0);
		assert_eq!(ring.claim(), 1);
		assert!(is_pending(&ring.get(1)));
	}

	#[test]
	fn publish_then_get_round_trips() {
		let ring = CommitRing::new(8, 1);
		let s = ring.claim();
		assert!(is_pending(&ring.get(s)));
		ring.publish(s, entry(s, false));
		assert_eq!(seq_of(ring.get(s)), Some(s));
		// A later, unclaimed sequence sharing no slot is still pending
		assert!(is_pending(&ring.get(s + 1)));
	}

	#[test]
	fn published_prefix_stops_at_the_first_gap() {
		let ring = CommitRing::new(8, 1);
		let (a, b, c) = (ring.claim(), ring.claim(), ring.claim());
		ring.publish(a, entry(a, true));
		ring.publish(c, entry(c, true));
		ring.advance_published();
		assert_eq!(ring.published(), a);
		ring.publish(b, entry(b, true));
		ring.advance_published();
		assert_eq!(ring.published(), c);
	}

	#[test]
	fn published_prefix_ignores_unclaimed_sequences() {
		let ring = CommitRing::new(8, 1);
		let a = ring.claim();
		ring.publish(a, entry(a, true));
		advance(&ring);
		advance(&ring);
		assert_eq!(ring.published(), a);
		assert_eq!(ring.completed(), a);
	}

	#[test]
	fn completed_prefix_stops_at_an_incomplete_entry() {
		let ring = CommitRing::new(8, 1);
		let entries: Vec<_> = (0..3)
			.map(|_| {
				let s = ring.claim();
				let e = entry(s, false);
				ring.publish(s, Arc::clone(&e));
				e
			})
			.collect();
		entries[0].done.store(true, Ordering::SeqCst);
		entries[2].done.store(true, Ordering::SeqCst);
		advance(&ring);
		assert_eq!(ring.published(), 3);
		assert_eq!(ring.completed(), 1);
		entries[1].done.store(true, Ordering::SeqCst);
		ring.advance_completed();
		assert_eq!(ring.completed(), 3);
	}

	#[test]
	fn completed_prefix_never_passes_the_published_prefix() {
		let ring = CommitRing::new(8, 1);
		let a = ring.claim();
		let b = ring.claim();
		ring.publish(a, entry(a, true));
		advance(&ring);
		assert_eq!(ring.completed(), a);
		// `b` is claimed but unpublished, so neither prefix may cover it
		ring.advance_completed();
		assert_eq!(ring.published(), a);
		assert_eq!(ring.completed(), a);
		ring.publish(b, entry(b, true));
		advance(&ring);
		assert_eq!(ring.completed(), b);
	}

	#[test]
	fn slots_are_reused_across_many_laps() {
		let ring = CommitRing::new(4, 1);
		for _ in 0..(ring.capacity() * 5) {
			let s = ring.claim();
			ring.publish(s, entry(s, true));
			advance(&ring);
			assert_eq!(ring.published(), s);
			assert_eq!(ring.completed(), s);
		}
		let last = ring.published();
		// The most recent lap is readable, every earlier lap is gone
		for s in (last - ring.capacity() + 1)..=last {
			assert_eq!(seq_of(ring.get(s)), Some(s));
		}
		for s in 1..=(last - ring.capacity()) {
			assert!(is_gone(&ring.get(s)), "sequence {s} should have been lapped");
		}
	}

	#[test]
	fn publish_waits_for_an_unpublished_previous_occupant() {
		// A committer is descheduled between claiming a sequence and
		// publishing it while a full lap of commits goes past. If the
		// lapping commit overwrote the slot first, the published prefix
		// would wedge behind it forever and one of the two entries would
		// be silently lost.
		let ring = Arc::new(CommitRing::new(4, 1));
		let held = ring.claim();
		let lapper = {
			let ring = Arc::clone(&ring);
			thread::spawn(move || {
				for _ in 0..ring.capacity() {
					let s = ring.claim();
					ring.publish(s, entry(s, true));
				}
			})
		};
		settle();
		assert!(!lapper.is_finished(), "the lapping publish must wait");
		ring.publish(held, entry(held, true));
		lapper.join().unwrap();
		advance(&ring);
		let last = held + ring.capacity();
		assert_eq!(ring.published(), last);
		assert_eq!(ring.completed(), last);
		assert!(is_gone(&ring.get(held)));
		assert_eq!(seq_of(ring.get(last)), Some(last));
	}

	#[test]
	fn publish_waits_for_an_incomplete_previous_occupant() {
		// If the lapping commit overwrote an in-flight entry, the completed
		// prefix would read the newer, complete entry in its place and
		// pass a commit whose merge version is not yet visible.
		let ring = Arc::new(CommitRing::new(2, 1));
		let first = entry(ring.claim(), false);
		ring.publish(first.seq, Arc::clone(&first));
		let s = ring.claim();
		ring.publish(s, entry(s, true));
		advance(&ring);
		assert_eq!(ring.published(), 2);
		assert_eq!(ring.completed(), 0);
		let lapper = {
			let ring = Arc::clone(&ring);
			thread::spawn(move || {
				let s = ring.claim();
				ring.publish(s, entry(s, true));
				s
			})
		};
		settle();
		assert!(!lapper.is_finished(), "the lapping publish must wait");
		assert_eq!(seq_of(ring.get(first.seq)), Some(first.seq));
		advance(&ring);
		assert_eq!(ring.completed(), 0);
		first.done.store(true, Ordering::SeqCst);
		let lapped = lapper.join().unwrap();
		advance(&ring);
		assert_eq!(ring.published(), lapped);
		assert_eq!(ring.completed(), lapped);
		assert!(is_gone(&ring.get(first.seq)));
	}

	#[test]
	fn completed_prefix_treats_a_lapped_slot_as_complete() {
		// Publish a full lap and overwrite its first slot before either
		// prefix has been advanced over it.
		let ring = CommitRing::new(2, 1);
		for _ in 0..3 {
			let s = ring.claim();
			ring.publish(s, entry(s, true));
		}
		assert!(is_gone(&ring.get(1)));
		advance(&ring);
		assert_eq!(ring.published(), 3);
		assert_eq!(ring.completed(), 3);
	}

	/// Publish `n` complete commits and advance both prefixes over them.
	fn commit_all(ring: &CommitRing<Entry>, n: u64) {
		for _ in 0..n {
			let s = ring.claim();
			ring.publish(s, entry(s, true));
		}
		advance(ring);
	}

	#[test]
	fn advance_taken_is_monotonic() {
		let ring = CommitRing::new(8, 1);
		commit_all(&ring, 7);
		ring.advance_taken(5);
		assert_eq!(ring.taken(), 5);
		ring.advance_taken(3);
		assert_eq!(ring.taken(), 5);
		ring.advance_taken(7);
		assert_eq!(ring.taken(), 7);
	}

	#[test]
	fn advance_taken_releases_retired_entries() {
		let ring = CommitRing::new(8, 1);
		commit_all(&ring, 5);
		assert_eq!(ring.occupied(), 5);
		ring.advance_taken(3);
		assert_eq!(ring.occupied(), 2);
		for s in 1..=3 {
			assert!(is_gone(&ring.get(s)), "sequence {s} should have been released");
		}
		assert_eq!(seq_of(ring.get(4)), Some(4));
		assert_eq!(seq_of(ring.get(5)), Some(5));
		// Releasing leaves both prefixes where they were
		advance(&ring);
		assert_eq!(ring.published(), 5);
		assert_eq!(ring.completed(), 5);
	}

	#[test]
	fn raise_taken_releases_nothing() {
		let ring = CommitRing::new(8, 1);
		commit_all(&ring, 5);
		ring.raise_taken(4);
		assert_eq!(ring.taken(), 4);
		assert_eq!(ring.occupied(), 5);
		assert_eq!(seq_of(ring.get(1)), Some(1));
	}

	#[test]
	fn release_retired_visits_at_most_the_limit_lowest_first() {
		let ring = CommitRing::new(64, 1);
		commit_all(&ring, 40);
		ring.raise_taken(40);
		assert!(ring.release_retired(16));
		assert_eq!(ring.occupied(), 24);
		assert!(is_gone(&ring.get(16)));
		assert_eq!(seq_of(ring.get(17)), Some(17));
		assert!(ring.release_retired(16));
		assert_eq!(ring.occupied(), 8);
		// The last eight are fewer than the limit
		assert!(!ring.release_retired(16));
		assert_eq!(ring.occupied(), 0);
		assert!(!ring.release_retired(16));
		assert_eq!(ring.taken(), 40);
	}

	#[test]
	fn release_retired_never_passes_the_watermark() {
		let ring = CommitRing::new(8, 1);
		commit_all(&ring, 6);
		ring.raise_taken(3);
		assert!(!ring.release_retired(100));
		assert_eq!(ring.occupied(), 3);
		assert_eq!(seq_of(ring.get(4)), Some(4));
		// The next call carries on from where the last one stopped
		ring.raise_taken(6);
		assert!(!ring.release_retired(100));
		assert_eq!(ring.occupied(), 0);
	}

	#[test]
	fn release_retired_skips_what_a_later_lap_overwrote() {
		let ring = CommitRing::new(4, 1);
		commit_all(&ring, 40);
		ring.raise_taken(40);
		// Only the last lap can hold entries, however many sequences were retired
		assert!(!ring.release_retired(4));
		assert_eq!(ring.occupied(), 0);
	}

	#[test]
	fn advance_taken_never_passes_the_completed_prefix() {
		let ring = CommitRing::new(8, 1);
		let (a, b) = (ring.claim(), ring.claim());
		ring.publish(a, entry(a, true));
		ring.publish(b, entry(b, false));
		advance(&ring);
		assert_eq!(ring.completed(), a);
		ring.advance_taken(b);
		assert_eq!(ring.taken(), a);
		// The in-flight entry is still readable
		assert_eq!(seq_of(ring.get(b)), Some(b));
		assert_eq!(ring.occupied(), 1);
	}

	#[test]
	fn a_released_slot_is_free_for_the_next_lap() {
		// A released occupant counts as complete, so the lapping publish
		// goes straight in rather than waiting
		let ring = CommitRing::new(2, 1);
		commit_all(&ring, 2);
		ring.advance_taken(2);
		assert_eq!(ring.occupied(), 0);
		commit_all(&ring, 2);
		assert_eq!(seq_of(ring.get(3)), Some(3));
		assert_eq!(seq_of(ring.get(4)), Some(4));
		assert_eq!(ring.completed(), 4);
	}

	#[test]
	fn release_skips_a_slot_owned_by_a_later_lap() {
		// Retire sequence 1 only after sequence 3 has taken its slot
		let ring = CommitRing::new(2, 1);
		commit_all(&ring, 3);
		ring.advance_taken(1);
		assert_eq!(ring.taken(), 1);
		assert_eq!(seq_of(ring.get(3)), Some(3));
		assert_eq!(ring.occupied(), 2);
	}

	#[test]
	fn release_only_visits_the_most_recent_lap() {
		// A large jump releases every slot still holding a retired entry
		let ring = CommitRing::new(4, 1);
		commit_all(&ring, 40);
		ring.advance_taken(40);
		assert_eq!(ring.taken(), 40);
		assert_eq!(ring.occupied(), 0);
	}

	#[test]
	fn publish_with_hands_over_a_held_previous_occupant() {
		let ring = CommitRing::new(2, 1);
		commit_all(&ring, 2);
		// Sequence 1 is released, so lapping it evicts nothing
		ring.advance_taken(1);
		let mut evicted = Vec::new();
		let s = ring.claim();
		ring.publish_with(s, entry(s, true), |p, e| evicted.push((p, e.seq)));
		assert!(evicted.is_empty());
		// Sequence 2 is still held, so lapping it hands it over first
		let s = ring.claim();
		ring.publish_with(s, entry(s, true), |p, e| evicted.push((p, e.seq)));
		assert_eq!(evicted, vec![(2, 2)]);
		assert!(is_gone(&ring.get(2)));
	}

	#[test]
	fn concurrent_commits_across_many_laps() {
		// Many committers share a tiny ring, forcing constant slot reuse,
		// while a reader checks that every entry it can see belongs to the
		// sequence it asked for, and a cleaner retires and releases entries
		// behind them.
		let threads: u64 = if cfg!(miri) {
			3
		} else {
			8
		};
		let per_thread: u64 = if cfg!(miri) {
			20
		} else {
			5_000
		};
		let ring = Arc::new(CommitRing::<Entry>::new(4, 1));
		let stop = Arc::new(AtomicBool::new(false));
		let reader = {
			let ring = Arc::clone(&ring);
			let stop = Arc::clone(&stop);
			thread::spawn(move || {
				while !stop.load(Ordering::Relaxed) {
					let hi = ring.published();
					for s in hi.saturating_sub(ring.capacity()).max(1)..=hi {
						if let SlotRead::Ready(e) = ring.get(s) {
							assert_eq!(e.seq, s, "slot returned another lap's entry");
						}
					}
					thread::yield_now();
				}
			})
		};
		let cleaner = {
			let ring = Arc::clone(&ring);
			let stop = Arc::clone(&stop);
			thread::spawn(move || {
				while !stop.load(Ordering::Relaxed) {
					ring.advance_taken(ring.completed().saturating_sub(1));
					thread::yield_now();
				}
			})
		};
		let writers: Vec<_> = (0..threads)
			.map(|_| {
				let ring = Arc::clone(&ring);
				thread::spawn(move || {
					for _ in 0..per_thread {
						let s = ring.claim();
						let e = entry(s, false);
						ring.publish(s, Arc::clone(&e));
						advance(&ring);
						// Our entry is in flight, so nothing may cover it
						assert!(ring.completed() < s, "completed prefix passed an in-flight entry");
						e.done.store(true, Ordering::SeqCst);
						advance(&ring);
					}
				})
			})
			.collect();
		for w in writers {
			w.join().unwrap();
		}
		stop.store(true, Ordering::Relaxed);
		reader.join().unwrap();
		cleaner.join().unwrap();
		advance(&ring);
		let total = threads * per_thread;
		assert_eq!(ring.published(), total);
		assert_eq!(ring.completed(), total);
		// Everything above the retired watermark is still readable
		for s in (ring.taken() + 1).max(total - ring.capacity() + 1)..=total {
			assert_eq!(seq_of(ring.get(s)), Some(s));
		}
		ring.advance_taken(total);
		assert_eq!(ring.occupied(), 0);
	}
}
