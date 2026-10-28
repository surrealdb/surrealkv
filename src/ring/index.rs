//! The recent commits' write keys, by key, for commit validation.
//!
//! A committer must abort if any commit in its conflict window that its snapshot does not see
//! wrote one of its write keys or locked reads. Visiting every entry of the window costs
//! O(window) per commit, and with thousands of short transactions in flight every commit's window
//! holds thousands of entries, so the total is quadratic in the commits in flight. Instead every
//! commit records its write keys here, by key hash, and a committer looks up only its own keys:
//! O(keys) per commit, whatever the window.
//!
//! A commit records its keys before it claims its ring sequence, and stores the sequence in its
//! entry once it has claimed it (see `CommitPipeline::publish`). The claim is an acquire and
//! release of the ring's counter, so a commit that claimed a sequence below a committer's own had
//! recorded its keys before the committer claimed: the committer finds every commit of its window
//! by looking its keys up, and waits for none of the others. A writer of one of its keys that has
//! not stored its sequence yet may have claimed one below the committer's or may claim one above
//! it, so the committer waits for that writer's sequence, which comes a few instructions after its
//! keys, and judges it then.
//!
//! An entry is only needed while some live or future window can still reach it: above the ring's
//! retired watermark. Each shard keeps two generations of its keys: new keys go into the current
//! one, and once every commit in the previous one is retired, the previous one is dropped whole
//! and the current one takes its place. Inserting into a key also drops the writers of that key
//! the watermark has passed.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;

use parking_lot::Mutex;

use super::queue::{CommitEntry, UNCLAIMED};
use super::sync::CachePadded;

/// The number of shards, a power of two. Enough that thousands of concurrent committers, each
/// locking a shard per key, rarely meet on one.
const SHARDS: usize = 256;

/// Where the shard index sits in a key hash. Away from the low bits, which pick the bucket of a
/// shard's map, and the top seven, which the map keeps as tags, so all of a shard's keys do not
/// share either.
const SHARD_SHIFT: u32 = 28;

/// How many keys a shard's current generation holds before, once its previous generation is
/// retired, it becomes the previous one. Small, so that retired entries are not held long, and
/// large enough that the rotation is rare next to the inserts.
const ROTATE_AT: usize = 64;

/// The hash a key is recorded and looked up by.
#[inline]
pub(crate) fn key_hash(key: &[u8]) -> u64 {
	xxhash_rust::xxh3::xxh3_64(key)
}

/// A hasher for keys that are already hashes.
#[derive(Default)]
struct KeyHashHasher(u64);

impl Hasher for KeyHashHasher {
	#[inline]
	fn finish(&self) -> u64 {
		self.0
	}

	fn write(&mut self, _: &[u8]) {
		unreachable!("only key hashes are hashed")
	}

	#[inline]
	fn write_u64(&mut self, n: u64) {
		self.0 = n;
	}
}

type Generation = HashMap<u64, Writers, BuildHasherDefault<KeyHashHasher>>;

/// The commits that wrote a key hash. Almost always one.
enum Writers {
	One(Arc<CommitEntry>),
	Many(Vec<Arc<CommitEntry>>),
}

/// Whether the commit of `entry` is at or below the retired watermark `taken`. One that has not
/// claimed its sequence yet is not: it claims one above every sequence claimed so far.
fn retired(entry: &CommitEntry, taken: u64) -> bool {
	let seq = entry.seq();
	seq != UNCLAIMED && seq <= taken
}

impl Writers {
	fn iter(&self) -> impl Iterator<Item = &Arc<CommitEntry>> {
		let (one, many) = match self {
			Self::One(entry) => (Some(entry), &[][..]),
			Self::Many(writers) => (None, &writers[..]),
		};
		one.into_iter().chain(many.iter())
	}

	/// Adds a writer, dropping those at or below the retired watermark `taken`, and from time to
	/// time those that aborted.
	fn push(&mut self, entry: &Arc<CommitEntry>, taken: u64) {
		match self {
			Self::One(e) if retired(e, taken) || e.is_aborted() => {
				*e = Arc::clone(entry);
			}
			Self::One(e) => {
				let first = Arc::clone(e);
				*self = Self::Many(vec![first, Arc::clone(entry)]);
			}
			Self::Many(writers) => {
				// A hot key keeps gaining writers while a long-lived transaction holds the
				// watermark back: drop the aborted ones too, which never conflict, each time the
				// list doubles, so that it stays near the size of its live writers.
				if writers.first().is_some_and(|e| retired(e, taken))
					|| writers.len().is_power_of_two()
				{
					writers.retain(|e| !retired(e, taken) && !e.is_aborted());
				}
				writers.push(Arc::clone(entry));
			}
		}
	}
}

#[derive(Default)]
struct Shard {
	current: Generation,
	previous: Generation,
	/// The newest sequence of the commits in `previous`, once every one of them has claimed its
	/// sequence. Worked out when a rotation first needs it, and kept, since the generation takes
	/// no more commits.
	previous_max: Option<u64>,
}

impl Shard {
	/// Whether every commit of the previous generation is at or below the retired watermark
	/// `taken`.
	fn previous_retired(&mut self, taken: u64) -> bool {
		if self.previous_max.is_none() {
			let mut max = 0;
			for entry in self.previous.values().flat_map(Writers::iter) {
				match entry.seq() {
					// Still to claim its sequence: it is not retired, and nor is the generation.
					UNCLAIMED => return false,
					seq => max = max.max(seq),
				}
			}
			self.previous_max = Some(max);
		}
		self.previous_max.is_some_and(|max| max <= taken)
	}

	/// Records that the commit of `entry` wrote the key hashed `hash`, and returns a retired
	/// generation for the caller to drop once the shard is unlocked.
	fn insert(&mut self, hash: u64, entry: &Arc<CommitEntry>, taken: u64) -> Option<Generation> {
		let retired = if self.current.len() >= ROTATE_AT && self.previous_retired(taken) {
			let current = std::mem::take(&mut self.current);
			self.previous_max = None;
			Some(std::mem::replace(&mut self.previous, current))
		} else {
			None
		};
		match self.current.entry(hash) {
			std::collections::hash_map::Entry::Occupied(mut writers) => {
				writers.get_mut().push(entry, taken);
			}
			std::collections::hash_map::Entry::Vacant(slot) => {
				slot.insert(Writers::One(Arc::clone(entry)));
			}
		}
		retired
	}
}

/// The write keys of the recent commits, by key hash.
pub(crate) struct WriteIndex {
	shards: Box<[CachePadded<Mutex<Shard>>]>,
}

impl WriteIndex {
	pub(crate) fn new() -> Self {
		Self {
			shards: (0..SHARDS).map(|_| CachePadded::new(Mutex::new(Shard::default()))).collect(),
		}
	}

	#[inline]
	fn shard(&self, hash: u64) -> &Mutex<Shard> {
		&self.shards[(hash >> SHARD_SHIFT) as usize & (SHARDS - 1)]
	}

	/// Records the write keys of `entry`, which claims its ring sequence once they are recorded.
	/// `taken` is the ring's retired watermark, below which nothing recorded is needed.
	pub(crate) fn insert(&self, entry: &Arc<CommitEntry>, taken: u64) {
		for &hash in entry.hashes() {
			let retired = self.shard(hash).lock().insert(hash, entry, taken);
			// Dropped once the shard is unlocked
			drop(retired);
		}
	}

	/// Whether a commit in `(window, seq)` that a snapshot at `start_seq` does not see wrote
	/// `key`, hashed `hash`. Commits still in flight count: they may yet commit. The committer
	/// asking has claimed `seq`.
	pub(crate) fn conflicts(
		&self,
		key: &[u8],
		hash: u64,
		seq: u64,
		start_seq: u64,
		window: u64,
	) -> bool {
		let in_window = |s: u64, other: &CommitEntry| {
			s > window && s < seq && !other.is_ignorable_at(start_seq)
		};
		// The writers that had not stored their sequence when the shard was read, judged once
		// they have, with the shard unlocked: they store it without taking the shard.
		let mut unclaimed: Vec<Arc<CommitEntry>> = Vec::new();
		{
			let shard = self.shard(hash).lock();
			for other in [&shard.current, &shard.previous]
				.into_iter()
				.filter_map(|g| g.get(&hash))
				.flat_map(Writers::iter)
			{
				if !other.writes(key) {
					continue;
				}
				match other.seq() {
					UNCLAIMED => unclaimed.push(Arc::clone(other)),
					s if in_window(s, other) => return true,
					_ => {}
				}
			}
		}
		unclaimed.iter().any(|other| in_window(other.claimed_seq(), other))
	}

	/// The number of key hashes held, for tests.
	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		self.shards
			.iter()
			.map(|s| {
				let s = s.lock();
				s.current.len() + s.previous.len()
			})
			.sum()
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeSet;

	use tokio::sync::Semaphore;

	use super::*;
	use crate::Key;

	fn entry(keys: &[Key]) -> Arc<CommitEntry> {
		Arc::new(CommitEntry::new(
			keys.to_vec(),
			Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
		))
	}

	/// Records the keys of `entry` and then its sequence, as a committer does.
	fn record(index: &WriteIndex, entry: &Arc<CommitEntry>, seq: u64, taken: u64) {
		index.insert(entry, taken);
		entry.set_seq(seq);
	}

	fn conflicts(index: &WriteIndex, keys: &[Key], seq: u64, start_seq: u64, window: u64) -> bool {
		keys.iter().any(|k| index.conflicts(k, key_hash(k), seq, start_seq, window))
	}

	#[test]
	fn a_lookup_conflicts_exactly_when_the_key_sets_overlap() {
		// A seeded xorshift: `fastrand` is not a dev-dependency on every target.
		let mut state = 7u64;
		let mut next = move |bound: u64| {
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			state % bound
		};
		for _ in 0..2000 {
			// A small key space, so that overlaps are common.
			let mut keys = || -> Vec<Key> {
				let n = next(41);
				(0..n).map(|_| (next(512) as u16).to_be_bytes().to_vec()).collect()
			};
			let (a, b) = (keys(), keys());
			let exact = !a.iter().collect::<BTreeSet<_>>().is_disjoint(&b.iter().collect());
			let index = WriteIndex::new();
			record(&index, &entry(&a), 1, 0);
			assert_eq!(conflicts(&index, &b, 2, 0, 0), exact, "{a:?} {b:?}");
		}
	}

	#[test]
	fn only_unseen_writers_inside_the_window_and_below_the_committer_conflict() {
		let key: Key = b"k".to_vec();
		let index = WriteIndex::new();
		let writer = entry(std::slice::from_ref(&key));
		record(&index, &writer, 5, 0);
		let at = |seq, start_seq, window| {
			conflicts(&index, std::slice::from_ref(&key), seq, start_seq, window)
		};
		// In flight, inside the window
		assert!(at(6, 0, 4));
		// At or below the window
		assert!(!at(6, 0, 5));
		// At or above the committer's own sequence
		assert!(!at(5, 0, 4));
		// Visible at the snapshot, or not yet
		writer.make_visible(10);
		assert!(!at(6, 10, 4));
		assert!(at(6, 9, 4));
		// Aborted
		let aborted = entry(std::slice::from_ref(&key));
		record(&index, &aborted, 7, 0);
		aborted.abort();
		assert!(!at(8, 10, 4));
	}

	/// A writer that recorded its keys and has not stored its sequence yet is judged by the
	/// sequence it then stores: below the committer's it conflicts, above it it does not.
	#[test]
	fn a_writer_still_claiming_is_judged_by_the_sequence_it_claims() {
		let key: Key = b"k".to_vec();
		for (claimed, expected) in [(5, true), (12, false)] {
			let index = WriteIndex::new();
			let writer = entry(std::slice::from_ref(&key));
			index.insert(&writer, 0);
			let claiming = {
				let writer = Arc::clone(&writer);
				std::thread::spawn(move || {
					std::thread::sleep(std::time::Duration::from_millis(20));
					writer.set_seq(claimed);
				})
			};
			assert_eq!(conflicts(&index, std::slice::from_ref(&key), 10, 0, 0), expected);
			claiming.join().unwrap();
		}
	}

	/// A generation is not dropped while a commit in it has yet to claim its sequence, which will
	/// be above the watermark.
	#[test]
	fn a_generation_with_a_writer_still_claiming_is_kept() {
		let index = WriteIndex::new();
		let hot = b"hot".to_vec();
		let shard_of_hot = (key_hash(&hot) >> SHARD_SHIFT) as usize & (SHARDS - 1);
		let claiming = entry(std::slice::from_ref(&hot));
		index.insert(&claiming, 0);
		// Fill the shard of `hot` far past two rotations, with the watermark past everything
		// else, while `claiming` has still not stored its sequence.
		let mut seq = 0;
		let mut i = 0u64;
		let mut filled = 0;
		while filled < 4 * ROTATE_AT {
			i += 1;
			let key = i.to_be_bytes().to_vec();
			if (key_hash(&key) >> SHARD_SHIFT) as usize & (SHARDS - 1) != shard_of_hot {
				continue;
			}
			seq += 1;
			record(&index, &entry(&[key]), seq, seq);
			filled += 1;
		}
		claiming.set_seq(seq + 1);
		assert!(conflicts(&index, std::slice::from_ref(&hot), seq + 2, 0, seq));
	}

	#[test]
	fn retired_writers_and_generations_are_dropped() {
		let index = WriteIndex::new();
		let mut seq = 0;
		// Many keys, each written once, while the watermark trails close behind.
		for i in 0..100_000u32 {
			seq += 1;
			record(&index, &entry(&[i.to_be_bytes().to_vec()]), seq, seq.saturating_sub(100));
		}
		// Each shard holds at most two generations of a bounded size.
		assert!(index.len() <= SHARDS * 2 * (ROTATE_AT + 1), "{}", index.len());
		// One key written over and over keeps only the writers above the watermark.
		let key = b"hot".to_vec();
		for _ in 0..1000 {
			seq += 1;
			record(&index, &entry(std::slice::from_ref(&key)), seq, seq - 1);
		}
		let hash = key_hash(&key);
		let shard = index.shard(hash).lock();
		let held: usize = [&shard.current, &shard.previous]
			.into_iter()
			.filter_map(|g| g.get(&hash))
			.map(|w| w.iter().count())
			.sum();
		assert!(held <= 3, "{held}");
	}
}
