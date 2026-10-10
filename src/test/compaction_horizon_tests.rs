//! The rule that decides which versions a compaction drops, checked against a model of the
//! readers: for every small input, a read at every sequence number that a reader can use gives
//! the same answer on the output of the compaction as on its input.
//!
//! A reader is a registered snapshot, or it reads at the visible sequence number, the horizon, or
//! above: the readers that have not registered, or have not started. The model answers a read
//! the way `Snapshot::get` does and uses none of the decisions of the iterator, so it is a check
//! of the rule and not a copy of it.

use std::cmp::Ordering;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::{Arc, Mutex};

use tempfile::TempDir;
use test_log::test;

use crate::compaction::compactor::{CompactionOptions, Compactor};
use crate::compaction::leveled::Strategy;
use crate::comparator::{BytewiseComparator, InternalKeyComparator};
use crate::iter::{BoxedLSMIterator, CompactionIterator};
use crate::{
	Comparator,
	InternalKey,
	InternalKeyKind,
	InternalKeyRef,
	Key,
	LSMIterator,
	Mode,
	Options,
	Result,
	Tree,
};

const KEYS: usize = 3;

/// A version of a key. A range delete covers the keys from its own up to `end`, excluding it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
	key: usize,
	seq: u64,
	kind: InternalKeyKind,
	end: usize,
}

#[derive(Clone, Debug)]
struct Case {
	entries: Vec<Entry>,
	snapshots: Vec<u64>,
	horizon: u64,
	bottom: bool,
}

type Range = (Key, Key, u64);

fn name(key: usize) -> Key {
	format!("k{key}").into_bytes()
}

fn index(user_key: &[u8]) -> usize {
	(user_key[1] - b'0') as usize
}

/// The entries of a table, in the order of its iterator.
struct VecIter {
	entries: Vec<(Vec<u8>, Vec<u8>)>,
	pos: usize,
	cmp: InternalKeyComparator,
}

impl LSMIterator for VecIter {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.pos = self
			.entries
			.partition_point(|(key, _)| self.cmp.compare(key, target) == Ordering::Less);
		Ok(self.valid())
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.pos = 0;
		Ok(self.valid())
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.pos = self.entries.len().saturating_sub(1);
		Ok(self.valid())
	}

	fn next(&mut self) -> Result<bool> {
		if self.valid() {
			self.pos += 1;
		}
		Ok(self.valid())
	}

	fn prev(&mut self) -> Result<bool> {
		self.pos = if self.pos == 0 {
			self.entries.len()
		} else {
			self.pos - 1
		};
		Ok(self.valid())
	}

	fn valid(&self) -> bool {
		self.pos < self.entries.len()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		InternalKeyRef::from_encoded(&self.entries[self.pos].0)
	}

	fn value_encoded(&self) -> Result<&[u8]> {
		Ok(&self.entries[self.pos].1)
	}
}

fn ranges_of(entries: &[Entry]) -> Vec<Range> {
	entries
		.iter()
		.filter(|e| e.kind == InternalKeyKind::RangeDelete)
		.map(|e| (name(e.key), name(e.end), e.seq))
		.collect()
}

/// Runs the compaction of `case`, which has one table, and returns what it keeps and the range
/// deletions it writes.
fn compact(case: &Case) -> (Vec<Entry>, Vec<Range>) {
	let cmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));
	let mut sorted = case.entries.clone();
	sorted.sort_by(|a, b| a.key.cmp(&b.key).then(b.seq.cmp(&a.seq)));
	let entries = sorted
		.iter()
		.map(|e| {
			let value = if e.kind == InternalKeyKind::RangeDelete {
				name(e.end)
			} else {
				b"v".to_vec()
			};
			(InternalKey::new(name(e.key), e.seq, e.kind).encode(), value)
		})
		.collect();
	let table: BoxedLSMIterator<'static> = Box::new(VecIter {
		entries,
		pos: 0,
		cmp: InternalKeyComparator::new(Arc::new(BytewiseComparator::default())),
	});
	let mut iter = CompactionIterator::new(
		vec![table],
		Arc::new(cmp) as Arc<dyn Comparator>,
		case.bottom,
		case.snapshots.clone(),
		case.horizon,
	)
	.with_range_deletions(ranges_of(&case.entries));
	let kept = iter
		.by_ref()
		.map(|item| {
			let (key, _) = item.unwrap();
			let key_index = index(&key.user_key);
			sorted
				.iter()
				.find(|e| e.key == key_index && e.seq == key.seq_num())
				.cloned()
				.expect("the iterator emits what it was given")
		})
		.collect();
	(kept, iter.active_range_deletions().to_vec())
}

/// What a reader at `read_seq` gets for `key`, the way `Snapshot::get` answers: the newest
/// version at or below `read_seq`, absent if it is a delete or a range delete at or below
/// `read_seq` covers it, and the level below, here a value older than every version, if there is
/// none. The answer is the sequence number of the version.
fn read(
	entries: &[Entry],
	ranges: &[Range],
	lower: bool,
	read_seq: u64,
	key: usize,
) -> Option<u64> {
	let user_key = name(key);
	let covered = |seq: u64| {
		ranges.iter().any(|(start, end, rseq)| {
			*rseq <= read_seq
				&& seq <= *rseq
				&& user_key.as_slice() >= start.as_slice()
				&& user_key.as_slice() < end.as_slice()
		})
	};
	let newest = entries.iter().filter(|e| e.key == key && e.seq <= read_seq).max_by_key(|e| e.seq);
	match newest {
		Some(e) if e.kind == InternalKeyKind::Set && !covered(e.seq) => Some(e.seq),
		Some(_) => None,
		None if lower && !covered(0) => Some(0),
		None => None,
	}
}

/// The sequence numbers a reader can read at: the snapshots, and every one from the horizon up.
fn read_points(case: &Case) -> Vec<u64> {
	let top = case.entries.iter().map(|e| e.seq).max().unwrap_or(0) + 1;
	let mut points = case.snapshots.clone();
	points.extend(case.horizon.min(top)..=top);
	points
}

/// Checks the output of the compaction of `case` against the model, and returns why it fails.
fn check(case: &Case) -> std::result::Result<(), String> {
	let (kept, out_ranges) = compact(case);
	let in_ranges = ranges_of(&case.entries);
	let lower = !case.bottom;
	let points = read_points(case);
	let fail = |what: String| Err(format!("{what}\n  case: {case:?}\n  kept: {kept:?}"));

	for key in 0..KEYS {
		let ordered: Vec<u64> = kept.iter().filter(|e| e.key == key).map(|e| e.seq).collect();
		if ordered.windows(2).any(|w| w[0] <= w[1]) {
			return fail(format!("the versions of k{key} are not newest first: {ordered:?}"));
		}
		for &read_seq in &points {
			let expected = read(&case.entries, &in_ranges, lower, read_seq, key);
			let got = read(&kept, &out_ranges, lower, read_seq, key);
			if expected != got {
				return fail(format!(
					"a reader at {read_seq} reads {got:?} of k{key} and read {expected:?} before"
				));
			}
		}
	}
	if kept.windows(2).any(|w| w[0].key > w[1].key) {
		return fail("the keys are not in order".into());
	}

	// Nothing is kept for nothing: a version that is kept is the answer of a reader. The rule
	// is sound but not minimal for a snapshot above the horizon, which the visible sequence
	// number's never going down rules out, and a delete may be kept to mask the level below.
	if case.snapshots.iter().all(|&s| s <= case.horizon) {
		for e in kept.iter().filter(|e| e.kind == InternalKeyKind::Set) {
			let needed = points
				.iter()
				.any(|&r| read(&case.entries, &in_ranges, lower, r, e.key) == Some(e.seq));
			if !needed {
				return fail(format!("k{} at {} is kept and no reader reads it", e.key, e.seq));
			}
		}
	}
	Ok(())
}

fn kind_of(n: u64) -> InternalKeyKind {
	match n % 3 {
		0 => InternalKeyKind::Set,
		1 => InternalKeyKind::Delete,
		_ => InternalKeyKind::RangeDelete,
	}
}

/// Every set of one to three versions of one key, with sequence numbers from 1 to 6, every kind
/// of each version, every set of snapshots from 1 to 6, every horizon from 0 to 7 and the
/// largest, at the bottom level and above it. A range delete covers the key itself and the ones
/// after it.
#[test]
fn compaction_keeps_every_version_a_reader_can_still_see() {
	let mut cases = 0u64;
	let horizons: Vec<u64> = (0..=7).chain([u64::MAX]).collect();
	for seqs in 1u32..64 {
		if !(1..=3).contains(&seqs.count_ones()) {
			continue;
		}
		let seqs: Vec<u64> = (1..=6).filter(|s| seqs & (1 << (s - 1)) != 0).collect();
		for kinds in 0..3u64.pow(seqs.len() as u32) {
			let entries: Vec<Entry> = seqs
				.iter()
				.enumerate()
				.map(|(i, &seq)| Entry {
					key: 0,
					seq,
					kind: kind_of(kinds / 3u64.pow(i as u32)),
					end: KEYS,
				})
				.collect();
			for snapshots in 0u32..64 {
				let snapshots: Vec<u64> =
					(1..=6).filter(|s| snapshots & (1 << (s - 1)) != 0).collect();
				for &horizon in &horizons {
					for bottom in [false, true] {
						let case = Case {
							entries: entries.clone(),
							snapshots: snapshots.clone(),
							horizon,
							bottom,
						};
						if let Err(why) = check(&case) {
							panic!("{why}");
						}
						cases += 1;
					}
				}
			}
		}
	}
	assert!(cases > 500_000, "{cases}");
}

struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}

	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}
}

/// Three keys, up to six versions with sequence numbers that no two share, range deletes that
/// cover keys other than their own, up to three snapshots.
fn random_case(seed: u64) -> Case {
	let mut rng = Rng((0x9E37_79B9_7F4A_7C15 ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03)) | 1);
	for _ in 0..4 {
		rng.next();
	}
	let top = 14;
	let mut seqs: Vec<u64> = (1..=top).collect();
	let count = 1 + rng.below(6) as usize;
	let mut entries = Vec::new();
	for _ in 0..count {
		let seq = seqs.swap_remove(rng.below(seqs.len() as u64) as usize);
		let key = rng.below(KEYS as u64) as usize;
		let kind = match rng.below(20) {
			0..=11 => InternalKeyKind::Set,
			12..=16 => InternalKeyKind::Delete,
			_ => InternalKeyKind::RangeDelete,
		};
		let end = key + 1 + rng.below((KEYS - key) as u64) as usize;
		entries.push(Entry {
			key,
			seq,
			kind,
			end,
		});
	}
	let mut snapshots: Vec<u64> = (0..rng.below(4)).map(|_| 1 + rng.below(top)).collect();
	snapshots.sort_unstable();
	snapshots.dedup();
	let horizon = match rng.below(8) {
		0 => u64::MAX,
		_ => rng.below(top + 2),
	};
	Case {
		entries,
		snapshots,
		horizon,
		bottom: rng.below(2) == 0,
	}
}

#[test]
fn compaction_keeps_every_version_a_reader_can_still_see_over_random_inputs() {
	for seed in 0..50_000 {
		if let Err(why) = check(&random_case(seed)) {
			panic!("seed {seed}: {why}");
		}
	}
}

/// A reader that registers between the compaction's read of the visible sequence number and its
/// read of the snapshot list is in the list, and the version it reads stays. Were the list read
/// first, the same reader would be in neither, and a publication in between would let the
/// compaction drop its version as superseded.
///
/// `B` is applied and not published when the compaction reads the horizon, and published by the
/// time a reader that began in between holds its snapshot.
#[test(tokio::test)]
async fn the_horizon_is_read_before_the_snapshot_list() {
	let dir = TempDir::new().unwrap();
	let options = Arc::new(Options {
		path: dir.path().to_path_buf(),
		level0_max_files: 2,
		flush_on_close: false,
		..Default::default()
	});
	let tree = Arc::new(Tree::new(Arc::clone(&options)).unwrap());
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	tree.core.is_closed.store(true, AtomicOrdering::SeqCst);

	let mut visible = Vec::new();
	for value in [&b"A"[..], &b"B"[..]] {
		let mut txn = tree.begin_with_mode(Mode::WriteOnly).unwrap();
		txn.set(&b"k"[..], value).unwrap();
		txn.commit().await.unwrap();
		tree.flush().unwrap();
		visible.push(tree.core.seq_num());
	}
	let inner = Arc::clone(&tree.core.inner);
	assert_eq!(inner.l0_file_count(), 2);
	// B is not published.
	inner.visible_seq_num.store(visible[0], AtomicOrdering::Release);

	let reader = Arc::new(Mutex::new(None));
	let mut compaction = CompactionOptions::from(&inner);
	{
		let (tree, reader, published) =
			(Arc::clone(&tree), Arc::clone(&reader), Arc::clone(&inner.visible_seq_num));
		let b = visible[1];
		compaction.after_horizon_hook = Some(Arc::new(move || {
			*reader.lock().unwrap() = Some(tree.begin_with_mode(Mode::ReadOnly).unwrap());
			published.store(b, AtomicOrdering::Release);
		}));
	}
	let strategy = Arc::new(Strategy::from_options(Arc::clone(&options)));
	Compactor::new(compaction, strategy).compact().unwrap();
	assert_eq!(inner.l0_file_count(), 0, "the compaction merged both tables");

	let reader = reader.lock().unwrap().take().expect("the reader began between the two reads");
	assert_eq!(reader.get(&b"k"[..]).unwrap(), Some(b"A".to_vec()));
	drop(reader);
	assert_eq!(
		tree.begin_with_mode(Mode::ReadOnly).unwrap().get(&b"k"[..]).unwrap(),
		Some(b"B".to_vec())
	);
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}
