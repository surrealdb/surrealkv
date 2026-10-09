//! The flusher encodes the values of a group in place and applies its batches by moving their keys
//! and values into the memtable, instead of copying every entry twice.
//!
//! What it must not change is what reaches the WAL and the memtable:
//!
//! * the WAL holds exactly the bytes it held when the flusher built a second batch per input batch
//!   (checked against goldens captured from that implementation, and against a reference that still
//!   builds the second batch),
//! * a batch that is applied is spent, so a group that rotates, re-appends, writes to L0 or hits a
//!   full arena in the middle must still apply every batch exactly once and never log or write a
//!   spent one.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use super::collect_transaction_all;
use crate::batch::Batch;
use crate::clock::LogicalClock;
use crate::memtable::{max_entry_bytes, MemTable};
use crate::ring::PipelineHook;
use crate::vlog::ValueLocation;
use crate::wal::parallel_recovery::decode_segment_batches;
use crate::{
	Error,
	InternalKeyKind,
	LSMIterator,
	Mode,
	Options,
	Transaction,
	Tree,
	Value,
	WriteOptions,
};

// ---------------------------------------------------------------------------
// a deterministic workload
// ---------------------------------------------------------------------------

/// Every reading of the clock is one more than the last, so the timestamp a commit stamps on its
/// range deletes is the same on every run.
#[derive(Debug, Default)]
struct StepClock(AtomicU64);

/// What `StepClock` reads at the first commit.
const CLOCK_BASE: u64 = 1_000_000;

impl LogicalClock for StepClock {
	fn now(&self) -> u64 {
		CLOCK_BASE + self.0.fetch_add(1, Ordering::SeqCst)
	}
}

/// splitmix64: a fixed sequence for a fixed seed, with no dependency on a crate's version.
struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
		let mut z = self.0;
		z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
		z ^ (z >> 31)
	}

	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}
}

#[derive(Clone, Debug)]
enum Op {
	Set(Vec<u8>, Vec<u8>, u64),
	Delete(Vec<u8>, u64),
	RangeDelete(Vec<u8>, Vec<u8>),
}

/// Value lengths on both sides of every varint width, which is where the size bookkeeping of the
/// in-place encoding could be off by one.
const EDGE_LENGTHS: [usize; 8] = [0, 1, 127, 128, 129, 16_383, 16_384, 70_000];

fn value_len(rng: &mut Rng) -> usize {
	match rng.below(100) {
		0..=5 => EDGE_LENGTHS[rng.below(EDGE_LENGTHS.len() as u64) as usize],
		6..=19 => 129 + rng.below(20_000) as usize,
		_ => rng.below(300) as usize,
	}
}

/// `txns` transactions of 1 to 6 operations each: sets (some empty), deletes and range deletes,
/// with explicit timestamps of many varint widths, and keys on both sides of 128 bytes.
fn workload(txns: usize) -> Vec<Vec<Op>> {
	let mut rng = Rng(0x5EED_0FA1_10C0_FFEE);
	(0..txns)
		.map(|t| {
			let n = 1 + rng.below(6) as usize;
			(0..n)
				.map(|i| {
					let mut key = format!("k{t:05}_{i}").into_bytes();
					let pad = rng.below(200) as usize;
					key.resize(key.len() + pad, b'x');
					let ts = (1u64 << (t % 50)) + i as u64;
					match rng.below(10) {
						0..=5 => {
							let len = value_len(&mut rng);
							let value = (0..len).map(|b| (b as u64 ^ rng.next()) as u8).collect();
							Op::Set(key, value, ts)
						}
						6 | 7 => Op::Delete(key, ts),
						8 => {
							let mut end = key.clone();
							end.push(0xFF);
							Op::RangeDelete(key, end)
						}
						_ => Op::Set(key, Vec::new(), ts),
					}
				})
				.collect()
		})
		.collect()
}

/// The batch a transaction hands the pipeline for `ops`, the `commit_index`-th commit. Range
/// deletes carry the commit timestamp, as `Transaction::commit` stamps them.
fn raw_batch(ops: &[Op], commit_index: u64, start_seq: u64) -> Batch {
	let mut batch = Batch::new(start_seq);
	for op in ops {
		match op {
			Op::Set(key, value, ts) => {
				batch.add_record(InternalKeyKind::Set, key.clone(), Some(value.clone()), *ts)
			}
			Op::Delete(key, ts) => {
				batch.add_record(InternalKeyKind::Delete, key.clone(), None, *ts)
			}
			Op::RangeDelete(start, end) => batch.add_record(
				InternalKeyKind::RangeDelete,
				start.clone(),
				Some(end.clone()),
				CLOCK_BASE + commit_index,
			),
		}
		.unwrap();
	}
	batch
}

/// What the flusher built before it encoded values in place: a second batch, entry by entry,
/// with every value wrapped in an inline `ValueLocation` (a range delete keeps its end key).
fn copied_batch(batch: &Batch) -> Batch {
	let mut processed = Batch::new(batch.starting_seq_num);
	for (_, entry, _, timestamp) in batch.entries_with_seq_nums().unwrap() {
		let value = if entry.kind == InternalKeyKind::RangeDelete {
			entry.value.clone()
		} else {
			entry.value.as_ref().map(|v| ValueLocation::with_inline_value(v.clone()).encode())
		};
		processed.add_record(entry.kind, entry.key.clone(), value, timestamp).unwrap();
	}
	processed
}

/// A record as `Batch::add_record` takes it.
type Rec = (InternalKeyKind, Vec<u8>, Option<Vec<u8>>, u64);

fn rec_batch(recs: &[Rec], start_seq: u64) -> Batch {
	let mut batch = Batch::new(start_seq);
	for (kind, key, value, ts) in recs {
		batch.add_record(*kind, key.clone(), value.clone(), *ts).unwrap();
	}
	batch
}

/// The smallest and the largest timestamp that takes each number of varint bytes, and zero.
fn varint_timestamps() -> Vec<u64> {
	let mut out = vec![0u64];
	for width in 1..=10u32 {
		let smallest = if width == 1 {
			1
		} else {
			1u64 << (7 * (width - 1))
		};
		let largest = if width == 10 {
			u64::MAX
		} else {
			(1u64 << (7 * width)) - 1
		};
		out.extend([smallest, largest]);
	}
	out
}

const KEY_LENGTHS: [usize; 9] = [1, 2, 126, 127, 128, 129, 300, 16_383, 16_384];

/// Value lengths around the points where `len` and `len + 2` (an inline `ValueLocation` adds a
/// flag and a version byte) take a different number of varint bytes.
const VALUE_LENGTHS: [usize; 13] =
	[0, 1, 2, 125, 126, 127, 128, 129, 16_381, 16_382, 16_383, 16_384, 70_000];

/// Records that put every encoding decision of the flusher on both sides: explicit timestamps of
/// every varint width on every kind of record (range deletes included), keys and values on both
/// sides of every length boundary, empty values, and a sweep of value lengths from 0 to 70 000.
fn edge_records() -> Vec<Rec> {
	let bytes = |len: usize, tag: usize| -> Vec<u8> {
		(0..len).map(|i| (tag.wrapping_mul(31).wrapping_add(i.wrapping_mul(7))) as u8).collect()
	};
	let mut recs: Vec<Rec> = Vec::new();
	for (n, ts) in varint_timestamps().into_iter().enumerate() {
		let klen = KEY_LENGTHS[n % KEY_LENGTHS.len()];
		let vlen = VALUE_LENGTHS[n % VALUE_LENGTHS.len()];
		let end_len = KEY_LENGTHS[(n + 4) % KEY_LENGTHS.len()];
		recs.push((InternalKeyKind::Set, bytes(klen, n), Some(bytes(vlen, n)), ts));
		recs.push((InternalKeyKind::Delete, bytes(klen, n + 1000), None, ts));
		recs.push((
			InternalKeyKind::RangeDelete,
			bytes(klen, n + 2000),
			Some(bytes(end_len, n + 3000)),
			ts,
		));
		recs.push((InternalKeyKind::Set, bytes(klen, n + 4000), Some(Vec::new()), ts));
		recs.push((InternalKeyKind::SoftDelete, bytes(klen, n + 5000), None, ts));
		// The other kinds that carry a value are wrapped like a set.
		recs.push((InternalKeyKind::SoftDelete, bytes(klen, n + 6000), Some(bytes(vlen, n)), ts));
	}
	for (i, klen) in KEY_LENGTHS.iter().enumerate() {
		for (j, vlen) in VALUE_LENGTHS.iter().enumerate() {
			let tag = 10_000 + i * 100 + j;
			recs.push((
				InternalKeyKind::Set,
				bytes(*klen, tag),
				Some(bytes(*vlen, tag)),
				(i * 31 + j) as u64,
			));
		}
	}
	for (n, vlen) in (0..=520usize).chain((521..=70_000).step_by(1_009)).enumerate() {
		recs.push((
			InternalKeyKind::Set,
			bytes(1 + n % 200, 20_000 + n),
			Some(bytes(vlen, 20_000 + n)),
			n as u64,
		));
	}
	recs
}

// ---------------------------------------------------------------------------
// the WAL, byte for byte
// ---------------------------------------------------------------------------

/// Every WAL segment of a database, as `(segment id, path)` in segment order.
fn wal_segments(dir: &Path) -> Vec<(u64, PathBuf)> {
	let mut segments = Vec::new();
	for entry in std::fs::read_dir(dir.join("wal")).unwrap() {
		let path = entry.unwrap().path();
		if path.extension().is_some_and(|ext| ext == "wal") {
			let id: u64 = path.file_stem().unwrap().to_str().unwrap().parse().unwrap();
			segments.push((id, path));
		}
	}
	segments.sort();
	segments
}

/// The WAL segments of a database that hold bytes, summed up: how many, how many bytes, and the
/// FNV-1a 64 hash of all of them in segment order. Opening a tree continues in a fresh segment,
/// which leaves the first one empty, so an empty segment is not counted.
#[derive(Debug, PartialEq, Eq)]
struct WalImage {
	segments: usize,
	len: u64,
	fnv: u64,
}

fn wal_image(dir: &Path) -> WalImage {
	let mut segments = wal_segments(dir);
	segments.retain(|(_, path)| std::fs::metadata(path).unwrap().len() > 0);
	let mut len = 0u64;
	let mut fnv = 0xcbf2_9ce4_8422_2325u64;
	for (_, path) in &segments {
		let bytes = std::fs::read(path).unwrap();
		len += bytes.len() as u64;
		for byte in bytes {
			fnv = (fnv ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
		}
	}
	WalImage {
		segments: segments.len(),
		len,
		fnv,
	}
}

/// Every WAL record, decoded the way recovery decodes it, in segment order.
fn wal_batches(dir: &Path) -> Vec<Batch> {
	wal_segments(dir)
		.into_iter()
		.flat_map(|(id, path)| decode_segment_batches(&path, id).unwrap().1)
		.collect()
}

/// Values above this many bytes go to the value log when it is enabled.
const VLOG_THRESHOLD: usize = 1000;

fn options(path: &Path, vlog: bool) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		// Crash semantics: nothing may be flushed on drop or close.
		flush_on_close: false,
		clock: Arc::new(StepClock::default()),
		enable_vlog: vlog,
		vlog_value_threshold: VLOG_THRESHOLD,
		..Default::default()
	})
}

/// Drops `tree` the way a crash does: no close, so nothing but what the commits appended is in
/// the directory. Must not await between the lock release and the drop.
fn crash(tree: Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.is_closed.store(true, Ordering::SeqCst);
	drop(tree);
}

/// A transaction that writes `ops`, ready to commit.
fn transaction_of(tree: &Tree, ops: &[Op]) -> Transaction {
	let mut txn = tree.begin().unwrap();
	for op in ops {
		match op {
			Op::Set(key, value, ts) => txn.set_at(key.clone(), value.clone(), *ts).unwrap(),
			Op::Delete(key, ts) => txn
				.delete_with_options(
					key.clone(),
					&WriteOptions::default().with_timestamp(Some(*ts)),
				)
				.unwrap(),
			Op::RangeDelete(start, end) => txn.delete_range(start.clone(), end.clone()).unwrap(),
		}
	}
	txn
}

/// Commits `ops` one transaction at a time and returns the WAL image after an unclean stop.
async fn wal_after_transactions(ops: &[Vec<Op>], vlog: bool) -> (WalImage, Vec<Batch>) {
	let dir = TempDir::new("alloc_diet").unwrap();
	let tree = Tree::new(options(dir.path(), vlog)).unwrap();
	for txn_ops in ops {
		transaction_of(&tree, txn_ops).commit().await.unwrap();
	}
	crash(tree);
	(wal_image(dir.path()), wal_batches(dir.path()))
}

/// Hands `count` batches, the `i`-th being `make(i, start_seq)`, to `flush_group` in groups of
/// the sizes `group_sizes` cycles through, and returns the WAL image after an unclean stop and
/// the records in it.
async fn wal_after_groups(
	count: usize,
	make: impl Fn(usize, u64) -> Batch,
	group_sizes: &[usize],
	vlog: bool,
) -> (WalImage, Vec<Batch>) {
	let dir = TempDir::new("alloc_diet").unwrap();
	let tree = Tree::new(options(dir.path(), vlog)).unwrap();
	let pipeline = &tree.core.commit_pipeline;
	let mut next = 0;
	let mut group_index = 0;
	while next < count {
		let size = group_sizes[group_index % group_sizes.len()].min(count - next);
		let mut group = Vec::new();
		for i in next..next + size {
			// The sequence number the batch is stamped with is that of its first entry, and
			// the entries of the batch are numbered on from it, as `flush_entries` does.
			let probe = make(i, 0);
			let start = pipeline.log_seq_num.fetch_add(u64::from(probe.count()), Ordering::SeqCst);
			group.push(make(i, start));
		}
		// Syncing costs an fsync, which says nothing about the bytes: do it now and then.
		pipeline.flush_group(&group, group_index % 16 == 0).await.unwrap();
		next += size;
		group_index += 1;
	}
	crash(tree);
	(wal_image(dir.path()), wal_batches(dir.path()))
}

/// The WAL of `workload(TXNS)` as the flusher wrote it when it built a second batch for every
/// input batch (captured from that implementation, before the WAL group append and the retire
/// watermark, and again from the tree that has them). A change to these means the on-disk bytes
/// changed.
const GOLDEN_WAL: WalImage = WalImage {
	segments: 1,
	len: 16_327_343,
	fnv: 0xabf9_ac75_e0ad_74cb,
};

/// The same, with the value log enabled at `VLOG_THRESHOLD`: values above it are logged as
/// pointers into the value log.
const GOLDEN_WAL_WITH_VLOG: WalImage = WalImage {
	segments: 1,
	len: 2_242_963,
	fnv: 0x5c96_9012_5934_3c7d,
};

/// The WAL of `edge_records()` in batches of 3 records, handed to `flush_group` in groups of
/// 1, 4, 2 and 7.
const GOLDEN_EDGE_WAL: WalImage = WalImage {
	segments: 1,
	len: 5_015_879,
	fnv: 0x2a4f_739e_4c27_b8ca,
};

/// The same, with the value log enabled at `VLOG_THRESHOLD`.
const GOLDEN_EDGE_WAL_WITH_VLOG: WalImage = WalImage {
	segments: 1,
	len: 1_124_529,
	fnv: 0x8433_b1aa_8680_e973,
};

const TXNS: usize = 3000;

/// The records the batches must have logged: what the copying flusher built, with the sequence
/// numbers running on from one batch to the next. Without the value log that is every value
/// wrapped inline.
fn assert_records_are_the_copied_batches(raw: &[Batch], records: &[Batch]) {
	assert_eq!(records.len(), raw.len(), "one WAL record per batch");
	for (i, (raw, record)) in raw.iter().zip(records).enumerate() {
		let expected = copied_batch(raw);
		assert_eq!(record.starting_seq_num, expected.starting_seq_num, "record {i}: sequence");
		assert_eq!(record.entries.len(), expected.entries.len(), "record {i}: entries");
		for (j, (got, want)) in record.entries.iter().zip(&expected.entries).enumerate() {
			assert_eq!(got.kind, want.kind, "record {i} entry {j}: kind");
			assert_eq!(got.key, want.key, "record {i} entry {j}: key");
			assert_eq!(got.timestamp, want.timestamp, "record {i} entry {j}: timestamp");
			assert_eq!(got.value, want.value, "record {i} entry {j}: value");
		}
		assert!(record.valueptrs.iter().all(Option::is_none), "record {i}: no batch pointers");
	}
}

/// The records that `wal_after_transactions` logged, against the batches the transactions made.
fn assert_transaction_records(ops: &[Vec<Op>], records: &[Batch]) {
	let mut start_seq = records[0].starting_seq_num;
	let raw: Vec<Batch> = ops
		.iter()
		.enumerate()
		.map(|(i, txn_ops)| {
			let batch = raw_batch(txn_ops, i as u64, start_seq);
			start_seq += u64::from(batch.count());
			batch
		})
		.collect();
	assert_records_are_the_copied_batches(&raw, records);
}

#[test(tokio::test)]
async fn the_wal_bytes_are_those_of_the_copying_flusher() {
	let ops = workload(TXNS);
	let (image, records) = wal_after_transactions(&ops, false).await;

	assert_transaction_records(&ops, &records);
	assert_eq!(image, GOLDEN_WAL, "the bytes of the WAL changed");
}

#[test(tokio::test)]
async fn the_wal_bytes_with_a_value_log_are_those_of_the_copying_flusher() {
	let ops = workload(TXNS);
	let (image, records) = wal_after_transactions(&ops, true).await;

	// Every record that is not a pointer is the copied batch, and every pointer is one.
	let mut start_seq = records[0].starting_seq_num;
	let mut pointers = 0;
	for (i, (txn_ops, record)) in ops.iter().zip(&records).enumerate() {
		let raw = raw_batch(txn_ops, i as u64, start_seq);
		let expected = copied_batch(&raw);
		assert_eq!(record.starting_seq_num, expected.starting_seq_num, "record {i}: sequence");
		assert_eq!(record.entries.len(), expected.entries.len(), "record {i}: entries");
		for (j, (got, want)) in record.entries.iter().zip(&expected.entries).enumerate() {
			assert_eq!((got.kind, &got.key, got.timestamp), (want.kind, &want.key, want.timestamp));
			let large = got.kind != InternalKeyKind::RangeDelete
				&& raw.entries[j].value.as_ref().is_some_and(|v| v.len() > VLOG_THRESHOLD);
			if large {
				let location = ValueLocation::decode(got.value.as_ref().unwrap()).unwrap();
				assert!(location.is_value_pointer(), "record {i} entry {j}: a pointer");
				pointers += 1;
			} else {
				assert_eq!(got.value, want.value, "record {i} entry {j}: value");
			}
		}
		start_seq += u64::from(record.count());
	}
	assert!(pointers > 100, "the workload has values above the threshold: {pointers}");
	assert_eq!(image, GOLDEN_WAL_WITH_VLOG, "the bytes of the WAL changed");
}

/// Groups of any size, with or without the value log, log the same bytes as one commit at a time:
/// the bytes depend on the batches, not on how the flusher happens to group them.
#[test(tokio::test)]
async fn groups_of_any_size_log_the_same_bytes_as_single_commits() {
	// Every workload is committed once per shape of group, so keep it short.
	let ops = workload(600);
	for vlog in [false, true] {
		let (single, _) = wal_after_transactions(&ops, vlog).await;
		for sizes in [&[1usize][..], &[1, 2, 3, 5, 8, 13], &[64, 1, 7]] {
			let (grouped, _) = wal_after_groups(
				ops.len(),
				|i, start| raw_batch(&ops[i], i as u64, start),
				sizes,
				vlog,
			)
			.await;
			assert_eq!(grouped, single, "vlog={vlog}, groups of {sizes:?}");
		}
	}
}

/// The records of `edge_records()`, three to a batch.
fn edge_batches() -> Vec<Vec<Rec>> {
	edge_records().chunks(3).map(<[Rec]>::to_vec).collect()
}

async fn edge_wal(vlog: bool, sizes: &[usize]) -> (WalImage, Vec<Batch>) {
	let batches = edge_batches();
	wal_after_groups(batches.len(), |i, start| rec_batch(&batches[i], start), sizes, vlog).await
}

#[test(tokio::test)]
async fn edge_records_log_the_bytes_of_the_copying_flusher() {
	let batches = edge_batches();
	let (image, records) = edge_wal(false, &[1, 4, 2, 7]).await;

	// The batches the group was made of, as the pipeline stamped them.
	let mut start_seq = records[0].starting_seq_num;
	let raw: Vec<Batch> = batches
		.iter()
		.map(|recs| {
			let batch = rec_batch(recs, start_seq);
			start_seq += u64::from(batch.count());
			batch
		})
		.collect();
	assert_records_are_the_copied_batches(&raw, &records);
	assert_eq!(image, GOLDEN_EDGE_WAL, "the bytes of the WAL changed");
}

#[test(tokio::test)]
async fn edge_records_with_a_value_log_log_the_bytes_of_the_copying_flusher() {
	let (image, records) = edge_wal(true, &[1, 4, 2, 7]).await;

	let pointers = records
		.iter()
		.flat_map(|r| &r.entries)
		.filter(|e| {
			e.kind != InternalKeyKind::RangeDelete
				&& e.value
					.as_ref()
					.is_some_and(|v| ValueLocation::decode(v).unwrap().is_value_pointer())
		})
		.count();
	assert!(pointers > 50, "the records have values above the threshold: {pointers}");
	assert_eq!(image, GOLDEN_EDGE_WAL_WITH_VLOG, "the bytes of the WAL changed");
}

#[test(tokio::test)]
async fn edge_records_log_the_same_bytes_in_groups_of_any_size() {
	for vlog in [false, true] {
		let (single, _) = edge_wal(vlog, &[1]).await;
		for sizes in [&[3usize][..], &[1, 4, 2, 7], &[100]] {
			assert_eq!(edge_wal(vlog, sizes).await.0, single, "vlog={vlog}, groups of {sizes:?}");
		}
	}
}

// ---------------------------------------------------------------------------
// what the tree holds
// ---------------------------------------------------------------------------

/// What reading the keys back must give after `txns` were applied in order.
fn model_of(txns: &[Vec<Op>]) -> BTreeMap<Vec<u8>, Value> {
	let mut model = BTreeMap::new();
	for op in txns.iter().flatten() {
		match op {
			Op::Set(key, value, _) => {
				model.insert(key.clone(), value.clone());
			}
			Op::Delete(key, _) => {
				model.remove(key);
			}
			Op::RangeDelete(start, end) => {
				model.retain(|key, _| key < start || key >= end);
			}
		}
	}
	model
}

/// A full scan of the tree.
fn scan(tree: &Tree) -> Vec<(Vec<u8>, Value)> {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let mut iter = txn.iter().unwrap();
	collect_transaction_all(&mut iter).unwrap()
}

fn assert_tree_equals(tree: &Tree, txns: &[Vec<Op>], what: &str) {
	let want: Vec<_> = model_of(txns).into_iter().collect();
	let got = scan(tree);
	let keys = |pairs: &[(Vec<u8>, Value)]| -> Vec<String> {
		pairs.iter().map(|(k, _)| String::from_utf8_lossy(k).into_owned()).collect()
	};
	assert_eq!(
		keys(&got),
		keys(&want),
		"{what}: keys (an empty key is a spent batch applied twice)"
	);
	for (got, want) in got.iter().zip(&want) {
		assert!(got.1 == want.1, "{what}: value of {:?}", String::from_utf8_lossy(&want.0));
	}
}

/// Commits the workload one transaction at a time, so every group is one batch, and reads the
/// tree back. The memtable got its keys and values by move, and the value log its large values,
/// so this is where a spent batch applied twice, or a value wrapped wrongly, shows.
async fn tree_after_transactions_matches_the_model(vlog: bool) {
	let ops = workload(TXNS);
	let dir = TempDir::new("alloc_diet").unwrap();
	let tree = Tree::new(options(dir.path(), vlog)).unwrap();
	for txn_ops in &ops {
		transaction_of(&tree, txn_ops).commit().await.unwrap();
	}

	let model: Vec<_> = model_of(&ops).into_iter().collect();
	assert!(model.len() > 1000, "the workload leaves something to compare");
	let got = scan(&tree);
	assert_eq!(got.len(), model.len(), "vlog={vlog}: number of keys");
	for (got, want) in got.iter().zip(&model) {
		assert_eq!(got.0, want.0, "vlog={vlog}: keys");
		assert_eq!(got.1.len(), want.1.len(), "vlog={vlog}: length of the value of {:?}", want.0);
		assert!(got.1 == want.1, "vlog={vlog}: value of {:?}", want.0);
	}
	crash(tree);
}

#[test(tokio::test)]
async fn the_tree_holds_what_the_transactions_wrote() {
	tree_after_transactions_matches_the_model(false).await;
}

#[test(tokio::test)]
async fn the_tree_holds_what_the_transactions_wrote_with_a_value_log() {
	tree_after_transactions_matches_the_model(true).await;
}

// ---------------------------------------------------------------------------
// the pipeline
// ---------------------------------------------------------------------------

/// A memtable that holds a few dozen of these keys.
const SMALL_MEMTABLE: usize = 8192;

/// Keys that were committed before the group below, so the group can delete them.
fn warm_up() -> Vec<Vec<Op>> {
	(0..20u64)
		.map(|i| vec![Op::Set(format!("w{i:02}").into_bytes(), vec![i as u8; 50], 1 + i)])
		.collect()
}

/// One group that straddles an arena fill (so the memtable rotates and the records that were
/// logged for the old one are logged again), holds a batch too big for any memtable (written to
/// L0), deletes, a range delete and empty values, and a batch that fits an empty memtable but
/// not what is left of the one it is applied to. No two of its batches write the same key.
fn rotating_group() -> Vec<Vec<Op>> {
	let value = |i: usize, j: usize| vec![(i + j) as u8; 100];
	let mut group = Vec::new();
	for i in 0..12 {
		group.push(
			(0..3).map(|j| Op::Set(format!("a{i:02}_{j}").into_bytes(), value(i, j), 7)).collect(),
		);
	}
	group.push(vec![
		Op::Delete(b"w03".to_vec(), 8),
		Op::RangeDelete(b"w10".to_vec(), b"w14".to_vec()),
		Op::Set(b"empty".to_vec(), Vec::new(), 9),
	]);
	group.push(vec![Op::Set(b"big".to_vec(), vec![0xB1; 3 * SMALL_MEMTABLE], 10)]);
	for i in 0..12 {
		group.push(
			(0..3).map(|j| Op::Set(format!("b{i:02}_{j}").into_bytes(), value(i, j), 7)).collect(),
		);
	}
	group.push(vec![Op::Set(b"near".to_vec(), vec![0xC2; SMALL_MEMTABLE * 3 / 4], 11)]);
	group.push(vec![Op::Set(b"tail".to_vec(), b"tail".to_vec(), 12)]);
	group
}

fn small_memtable_options(path: &Path, vlog: bool) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		flush_on_close: false,
		max_memtable_size: SMALL_MEMTABLE,
		enable_vlog: vlog,
		vlog_value_threshold: VLOG_THRESHOLD,
		// Keep write stalls out of the way.
		memtable_stall_threshold: 1000,
		level0_max_files: 10_000,
		l0_stall_threshold: 10_000,
		..Default::default()
	})
}

/// Stops the flush of immutable memtables, so that no WAL segment is deleted under a test that
/// counts the records in them.
async fn stop_background_tasks(tree: &Tree) {
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
}

/// Counts the apply rounds of every group: more than one per group means it was repaired by a
/// rotation, a re-append or a write to L0.
fn count_apply_rounds(tree: &Tree) -> Arc<AtomicUsize> {
	let rounds = Arc::new(AtomicUsize::new(0));
	let counter = Arc::clone(&rounds);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if matches!(point, PipelineHook::BeforeApplyRound { .. }) {
			counter.fetch_add(1, Ordering::SeqCst);
		}
	})));
	rounds
}

/// The batches of `group` as `flush_entries` hands them to `flush_group`: numbered on from the
/// next sequence number.
fn stamped(tree: &Tree, group: &[Vec<Op>]) -> Vec<Batch> {
	let pipeline = &tree.core.commit_pipeline;
	group
		.iter()
		.enumerate()
		.map(|(i, ops)| {
			let start = pipeline.log_seq_num.fetch_add(ops.len() as u64, Ordering::SeqCst);
			raw_batch(ops, i as u64, start)
		})
		.collect()
}

/// `flush_group` leaves publishing to the flusher.
fn publish_all(tree: &Tree) {
	let pipeline = &tree.core.commit_pipeline;
	tree.core
		.inner
		.visible_seq_num
		.store(pipeline.log_seq_num.load(Ordering::SeqCst) - 1, Ordering::SeqCst);
}

/// How many records the WAL holds for each starting sequence number.
fn records_by_sequence(dir: &Path) -> BTreeMap<u64, usize> {
	let mut counts = BTreeMap::new();
	for record in wal_batches(dir) {
		*counts.entry(record.starting_seq_num).or_insert(0) += 1;
	}
	counts
}

/// A spent batch, logged or written to L0, would carry empty keys.
fn assert_no_spent_record(dir: &Path) {
	for record in wal_batches(dir) {
		assert!(
			record.entries.iter().all(|e| !e.key.is_empty()),
			"the record at sequence {} has an empty key: a spent batch was logged",
			record.starting_seq_num
		);
	}
}

/// What reading `txns` back from a store that was opened on `dir` after an unclean stop gives.
async fn assert_recovers_to(dir: &Path, vlog: bool, txns: &[Vec<Op>], what: &str) {
	let reopened = Tree::new(small_memtable_options(dir, vlog)).unwrap();
	assert_tree_equals(&reopened, txns, what);
	crash(reopened);
}

/// The group goes through `flush_group` in one piece, as the flusher would hand it over.
#[test(tokio::test)]
async fn a_group_that_rotates_overflows_and_goes_to_l0_applies_every_batch_once() {
	let dir = TempDir::new("alloc_diet").unwrap();
	let tree = Tree::new(small_memtable_options(dir.path(), false)).unwrap();
	for txn_ops in &warm_up() {
		transaction_of(&tree, txn_ops).commit().await.unwrap();
	}
	let rounds = count_apply_rounds(&tree);

	let group = rotating_group();
	let batches = stamped(&tree, &group);
	tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();
	publish_all(&tree);

	// The caller's batches are still whole: only the copies the group works on were spent.
	for (batch, ops) in batches.iter().zip(&group) {
		assert_eq!(batch.count() as usize, ops.len());
		assert!(batch.entries.iter().all(|e| !e.key.is_empty()));
	}
	assert!(
		rounds.load(Ordering::SeqCst) >= 4,
		"the group was repaired by rotations and a write to L0: {} apply rounds",
		rounds.load(Ordering::SeqCst)
	);
	let all: Vec<_> = warm_up().into_iter().chain(group).collect();
	assert_tree_equals(&tree, &all, "flush_group");
	assert_no_spent_record(dir.path());
	crash(tree);
}

/// The same group through the commit ring, so the flusher itself forms it: on a current-thread
/// runtime every spawned commit is accepted before the flusher runs, and they are one group, in
/// spawn order.
#[test(tokio::test)]
async fn the_flusher_applies_a_repaired_group_of_commits_exactly_once() {
	let dir = TempDir::new("alloc_diet").unwrap();
	let tree = Arc::new(Tree::new(small_memtable_options(dir.path(), false)).unwrap());
	for txn_ops in &warm_up() {
		transaction_of(&tree, txn_ops).commit().await.unwrap();
	}
	let rounds = count_apply_rounds(&tree);

	let group = rotating_group();
	let handles: Vec<_> = group
		.iter()
		.map(|ops| {
			let mut txn = transaction_of(&tree, ops);
			tokio::spawn(async move { txn.commit().await })
		})
		.collect();
	for handle in handles {
		handle.await.unwrap().unwrap();
	}

	assert!(
		rounds.load(Ordering::SeqCst) >= 4,
		"the group was repaired by rotations and a write to L0: {} apply rounds",
		rounds.load(Ordering::SeqCst)
	);
	// The flusher keeps its buffers for the commits after the repaired group, which must find
	// nothing of it in them.
	let after: Vec<Vec<Op>> = (0..5u64)
		.map(|i| vec![Op::Set(format!("z{i}").into_bytes(), vec![i as u8; 30], 20 + i)])
		.collect();
	for txn_ops in &after {
		transaction_of(&tree, txn_ops).commit().await.unwrap();
	}
	let all: Vec<_> = warm_up().into_iter().chain(group).chain(after).collect();
	assert_tree_equals(&tree, &all, "flusher");
	assert_no_spent_record(dir.path());
	tree.core.commit_pipeline.set_hook(None);
	let tree = Arc::into_inner(tree).unwrap();
	crash(tree);
}

/// An oversized batch in the middle of a group is written to L0, which seals the WAL segment the
/// group was logged in. The batches before it were applied (they are spent, and their records
/// are where the memtable that holds them expects them), the oversized one needs no second
/// record, and the batches after it are logged again, in the segment of the memtable that takes
/// them. Nothing is logged for a batch that was moved out.
#[test(tokio::test)]
async fn an_oversized_batch_in_the_middle_of_a_group_logs_only_the_batches_after_it_again() {
	let dir = TempDir::new("alloc_diet").unwrap();
	let tree = Tree::new(small_memtable_options(dir.path(), false)).unwrap();
	stop_background_tasks(&tree).await;

	let small = |name: &str| vec![Op::Set(name.as_bytes().to_vec(), vec![1u8; 40], 1)];
	let group = vec![
		small("m0"),
		small("m1"),
		vec![Op::Set(b"oversized".to_vec(), vec![0xEE; 3 * SMALL_MEMTABLE], 2)],
		small("m3"),
		small("m4"),
	];
	let batches = stamped(&tree, &group);
	tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();
	publish_all(&tree);

	let first = batches[0].starting_seq_num;
	assert_eq!(
		records_by_sequence(dir.path()),
		BTreeMap::from([
			(first, 1),
			(first + 1, 1),
			(first + 2, 1),
			(first + 3, 2),
			(first + 4, 2)
		]),
		"the batches applied before the oversized one are logged once, the oversized one once, \
		 and the ones after it again"
	);
	assert_no_spent_record(dir.path());
	// Two tables: the oversized batch's own, and the memtable that held the batches before it,
	// which the direct-to-L0 write flushes first so that it cannot shadow the new table.
	assert_eq!(tree.core.inner.level_manifest.read().unwrap().get_all_tables().len(), 2);
	assert_tree_equals(&tree, &group, "oversized in the middle");
	crash(tree);
	assert_recovers_to(dir.path(), false, &group, "recovery").await;
}

/// The length of the (encoded) value of a one-key batch with a key of `key_len` bytes that does
/// not fit what is left of a memtable that `first` was applied to, but fits an empty one. Found by
/// probing memtables of the size the tree uses.
fn overflowing_value_len(first: &Batch, key_len: usize) -> usize {
	let fresh = MemTable::new(SMALL_MEMTABLE);
	let used = MemTable::new(SMALL_MEMTABLE);
	used.add(first).unwrap();
	let largest_fitting_fresh = (0..=SMALL_MEMTABLE)
		.rev()
		.find(|vlen| fresh.can_fit(max_entry_bytes(key_len, *vlen)))
		.expect("some batch fits an empty memtable");
	assert!(
		!used.can_fit(max_entry_bytes(key_len, largest_fitting_fresh)),
		"the first batch leaves less room than an empty memtable has"
	);
	largest_fitting_fresh
}

/// A batch that does not fit what is left of the memtable stops the apply after batches that were
/// applied (and spent). The memtable rotates, and the same batch, untouched, goes to the new one.
#[test(tokio::test)]
async fn a_batch_that_overflows_the_memtable_in_the_middle_of_a_group_is_applied_whole_to_the_next()
{
	let dir = TempDir::new("alloc_diet").unwrap();
	let tree = Tree::new(small_memtable_options(dir.path(), false)).unwrap();
	stop_background_tasks(&tree).await;
	let capacity = tree.core.inner.active_memtable.read().unwrap().arena_capacity();
	assert!(capacity >= SMALL_MEMTABLE, "the arena is as big as the memtable size");

	let first_ops = vec![Op::Set(b"first".to_vec(), vec![0xA1; 10], 1)];
	let key = b"second".to_vec();
	// What the group logs is encoded, so what is probed is too.
	let first = copied_batch(&raw_batch(&first_ops, 0, 1));
	let value_len = overflowing_value_len(&first, key.len()) - 2;
	let second_ops = vec![Op::Set(key, vec![0xB2; value_len], 2)];

	let group = vec![first_ops, second_ops, vec![Op::Set(b"third".to_vec(), vec![0xC3; 20], 3)]];
	let batches = stamped(&tree, &group);
	tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();
	publish_all(&tree);

	let start = batches[0].starting_seq_num;
	assert_eq!(
		records_by_sequence(dir.path()),
		BTreeMap::from([(start, 1), (start + 1, 2), (start + 2, 2)]),
		"the first batch is logged once, the ones that went to the next memtable twice"
	);
	assert_no_spent_record(dir.path());
	assert_eq!(
		tree.core.inner.immutable_memtables.read().unwrap().iter().count(),
		1,
		"the memtable rotated once"
	);
	{
		let active = tree.core.inner.active_memtable.read().unwrap();
		assert!(active.get(b"second", None).is_some(), "the retried batch is in the new memtable");
		assert!(active.get(b"third", None).is_some());
		assert!(active.get(b"first", None).is_none(), "the first batch is not applied twice");
	}
	assert_tree_equals(&tree, &group, "overflow in the middle");
	crash(tree);
	assert_recovers_to(dir.path(), false, &group, "recovery").await;
}

/// A batch below `max_memtable_size` that still does not fit an empty arena is written to L0 by
/// the flusher, with its contents whole.
#[test(tokio::test)]
async fn a_batch_that_does_not_fit_an_empty_memtable_is_written_to_l0_whole() {
	let dir = TempDir::new("alloc_diet").unwrap();
	let tree = Tree::new(small_memtable_options(dir.path(), false)).unwrap();
	stop_background_tasks(&tree).await;

	// Below `max_memtable_size`, so not oversized, and above what an empty arena has left.
	let key = b"snug".to_vec();
	let fresh = MemTable::new(SMALL_MEMTABLE);
	let value_len = (0..=SMALL_MEMTABLE)
		.rev()
		.find(|vlen| {
			let estimate = max_entry_bytes(key.len(), vlen + 2);
			estimate <= SMALL_MEMTABLE as u64 && !fresh.can_fit(estimate)
		})
		.expect("a batch between the arena and the memtable size");
	let group = vec![
		vec![Op::Set(b"before".to_vec(), vec![1u8; 30], 1)],
		vec![Op::Set(key, vec![0xD4; value_len], 2)],
		vec![Op::Set(b"after".to_vec(), vec![2u8; 30], 3)],
	];
	let batches = stamped(&tree, &group);
	tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();
	publish_all(&tree);

	// Two tables: the batch's own, and the memtable that held "before", which the direct-to-L0
	// write flushes first so that it cannot shadow the new table.
	assert_eq!(tree.core.inner.level_manifest.read().unwrap().get_all_tables().len(), 2);
	assert_no_spent_record(dir.path());
	assert_tree_equals(&tree, &group, "a batch for L0 that fits no arena");
	crash(tree);
	assert_recovers_to(dir.path(), false, &group, "recovery").await;
}

/// With the value log the large values are replaced by pointers before the size of a batch is
/// taken, so a batch that is oversized as written is an ordinary batch to the memtable, and is
/// not written to L0. Mixed with inline values, across a rotation, and read back after an unclean
/// stop (the value log is flushed before the WAL record that points into it).
#[test(tokio::test)]
async fn a_value_log_group_is_sized_after_its_values_are_replaced_by_pointers() {
	for vlog in [true, false] {
		let dir = TempDir::new("alloc_diet").unwrap();
		let tree = Tree::new(small_memtable_options(dir.path(), vlog)).unwrap();
		stop_background_tasks(&tree).await;

		let value = |i: usize, len: usize| vec![i as u8 ^ 0x5A; len];
		let mut group = Vec::new();
		for i in 0..10 {
			// Below and above the threshold, and one of each in a batch.
			group.push(vec![
				Op::Set(format!("s{i:02}a").into_bytes(), value(i, 90), 1),
				Op::Set(format!("s{i:02}b").into_bytes(), value(i, VLOG_THRESHOLD + 1 + i), 1),
			]);
		}
		group.push(vec![Op::Set(b"huge".to_vec(), value(11, 3 * SMALL_MEMTABLE), 2)]);
		for i in 0..10 {
			group.push(vec![Op::Set(
				format!("t{i:02}").into_bytes(),
				value(i, VLOG_THRESHOLD + 200 * i),
				1,
			)]);
		}
		group.push(vec![Op::Delete(b"s03a".to_vec(), 5)]);
		let batches = stamped(&tree, &group);
		tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();
		publish_all(&tree);

		let tables = tree.core.inner.level_manifest.read().unwrap().get_all_tables().len();
		// Without the value log the huge value goes to L0, and the direct-to-L0 write flushes
		// the memtable that held the batches before it first (one table more).
		let expected = usize::from(!vlog) * 2;
		assert_eq!(
			tables, expected,
			"vlog={vlog}: only without the value log is the huge value too big for a memtable"
		);
		assert_no_spent_record(dir.path());
		assert_tree_equals(&tree, &group, &format!("vlog={vlog}"));
		crash(tree);
		assert_recovers_to(dir.path(), vlog, &group, &format!("vlog={vlog}, recovery")).await;
	}
}

/// The first batch of a group that does not fit what is left of a non-empty memtable is not
/// logged and then applied to the next one: the memtable rotates first, from the estimate the
/// group computed once, so that every batch of the group is logged exactly once.
#[test(tokio::test)]
async fn a_group_whose_first_batch_does_not_fit_rotates_before_it_is_logged() {
	let dir = TempDir::new("alloc_diet").unwrap();
	let tree = Tree::new(small_memtable_options(dir.path(), false)).unwrap();
	stop_background_tasks(&tree).await;

	let group = vec![
		vec![Op::Set(b"g0".to_vec(), vec![0x10; 3000], 1)],
		vec![Op::Set(b"g1".to_vec(), vec![0x11; 50], 1)],
		vec![Op::Set(b"g2".to_vec(), vec![0x12; 50], 1)],
	];
	let estimate = max_entry_bytes(2, 3000 + 2);
	let mut filled = Vec::new();
	for i in 0u64.. {
		if !tree.core.inner.active_memtable.read().unwrap().can_fit(estimate) {
			break;
		}
		let ops = vec![Op::Set(format!("f{i:03}").into_bytes(), vec![i as u8; 100], 1)];
		transaction_of(&tree, &ops).commit().await.unwrap();
		filled.push(ops);
	}
	assert!(!filled.is_empty(), "the memtable was not filled by anything");
	let rotations = tree.core.inner.immutable_memtables.read().unwrap().iter().count();
	assert_eq!(rotations, 0, "filling the memtable did not rotate it");

	let batches = stamped(&tree, &group);
	tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();
	publish_all(&tree);

	let start = batches[0].starting_seq_num;
	let counts = records_by_sequence(dir.path());
	for k in 0..3 {
		assert_eq!(counts[&(start + k)], 1, "batch {k} is logged once");
	}
	assert_eq!(tree.core.inner.immutable_memtables.read().unwrap().iter().count(), 1);
	assert_no_spent_record(dir.path());
	let all: Vec<_> = filled.into_iter().chain(group).collect();
	assert_tree_equals(&tree, &all, "rotation before the first batch");
	crash(tree);
	assert_recovers_to(dir.path(), false, &all, "recovery").await;
}

/// Commits `size` single-key transactions, all spawned before the flusher is polled: on the
/// current-thread runtime each runs up to its verdict await, so every one is accepted into the ring
/// before the flusher task runs, and the call is one group.
async fn commit_round(tree: &Arc<Tree>, round: usize, size: usize) {
	let handles: Vec<_> = (0..size)
		.map(|i| {
			let tree = Arc::clone(tree);
			tokio::spawn(async move {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("r{round:02}_{i:03}").into_bytes(), vec![round as u8; 20 + i])
					.unwrap();
				txn.commit().await
			})
		})
		.collect();
	for handle in handles {
		handle.await.unwrap().unwrap();
	}
}

/// The flusher reuses its buffers for the next group. Groups that grow and shrink must each be
/// exactly the commits of their round: a buffer that kept something of the group before would
/// log it again, wake the wrong waiter or publish the wrong sequence number.
#[test(tokio::test)]
async fn every_group_is_made_of_the_commits_of_its_own_round_only() {
	let dir = TempDir::new("alloc_diet").unwrap();
	let tree = Arc::new(Tree::new(options(dir.path(), false)).unwrap());
	let sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
	let sink = Arc::clone(&sizes);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::AfterWalSync {
			batches,
		} = point
		{
			sink.lock().unwrap().push(batches);
		}
	})));

	let rounds = [16usize, 1, 200, 2, 64, 1, 1, 33, 3, 128, 1];
	for (round, size) in rounds.iter().enumerate() {
		commit_round(&tree, round, *size).await;
	}
	assert_eq!(*sizes.lock().unwrap(), rounds, "one group per round");

	let total: usize = rounds.iter().sum();
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for (round, size) in rounds.iter().enumerate() {
			for i in 0..*size {
				let got = txn.get(format!("r{round:02}_{i:03}").as_bytes()).unwrap();
				assert_eq!(got, Some(vec![round as u8; 20 + i]), "round {round} commit {i}");
			}
		}
	}
	tree.core.commit_pipeline.set_hook(None);
	let tree = Arc::into_inner(tree).unwrap();
	crash(tree);
	let records = wal_batches(dir.path());
	assert_eq!(records.len(), total, "one record per commit, no record twice");
	let mut starts: Vec<u64> = records.iter().map(|r| r.starting_seq_num).collect();
	starts.dedup();
	assert_eq!(starts.len(), total);
	assert!(starts.windows(2).all(|w| w[0] < w[1]), "the records are in sequence order");
	assert_no_spent_record(dir.path());
}

/// A value log that cannot take a value in the middle of a group fails the group: its waiters
/// get an error and none of it is logged or readable, as with a WAL append that fails. Batches
/// before the failure were already encoded in place, and nothing reads them again. The writer is
/// not poisoned: once the value log works again, later commits go through.
#[test(tokio::test)]
async fn a_value_log_failure_in_the_middle_of_a_group_fails_the_group_and_logs_none_of_it() {
	let dir = TempDir::new("alloc_diet").unwrap();
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		flush_on_close: false,
		enable_vlog: true,
		vlog_value_threshold: VLOG_THRESHOLD,
		// Every value after the first goes to a new file.
		vlog_max_file_size: 1,
		..Default::default()
	});
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let large = |c: u8| vec![c; 2 * VLOG_THRESHOLD];

	let first = vec![Op::Set(b"first".to_vec(), large(1), 1)];
	transaction_of(&tree, &first).commit().await.unwrap();
	let records_before = wal_batches(dir.path()).len();

	// The value log directory is gone: the next file cannot be created.
	let moved = dir.path().join("vlog_moved");
	std::fs::rename(opts.vlog_dir(), &moved).unwrap();
	let doomed = [
		vec![Op::Set(b"inline".to_vec(), b"small".to_vec(), 2)],
		vec![Op::Set(b"large".to_vec(), large(2), 3), Op::Set(b"other".to_vec(), large(3), 3)],
		vec![Op::Set(b"after".to_vec(), b"small".to_vec(), 4)],
	];
	let handles: Vec<_> = doomed
		.iter()
		.map(|ops| {
			let mut txn = transaction_of(&tree, ops);
			tokio::spawn(async move { txn.commit().await })
		})
		.collect();
	for handle in handles {
		assert!(handle.await.unwrap().is_err(), "the group fails as a whole");
	}
	assert_eq!(wal_batches(dir.path()).len(), records_before, "none of the group is logged");
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for key in ["inline", "large", "other", "after"] {
			assert_eq!(txn.get(key.as_bytes()).unwrap(), None, "{key} was never acknowledged");
		}
	}

	// The value log works again, and the writer is not poisoned.
	std::fs::rename(&moved, opts.vlog_dir()).unwrap();
	let later = vec![Op::Set(b"later".to_vec(), large(4), 5)];
	transaction_of(&tree, &later).commit().await.unwrap();
	assert_eq!(wal_batches(dir.path()).len(), records_before + 1);
	assert_no_spent_record(dir.path());

	let tree = Arc::into_inner(tree).unwrap();
	crash(tree);
	let reopened = Tree::new(opts).unwrap();
	{
		let txn = reopened.begin_with_mode(Mode::ReadOnly).unwrap();
		assert_eq!(txn.get(b"first").unwrap(), Some(large(1)));
		assert_eq!(txn.get(b"later").unwrap(), Some(large(4)));
		for key in ["inline", "large", "other", "after"] {
			assert_eq!(txn.get(key.as_bytes()).unwrap(), None, "{key} after recovery");
		}
	}
	crash(reopened);
}

// ---------------------------------------------------------------------------
// MemTable::add_owned
// ---------------------------------------------------------------------------

/// A batch of every shape the flusher applies, with its values encoded the way
/// `encode_values` leaves them: two versions of one key, an empty value, a delete, a soft
/// delete, a range delete (whose value is its end key, unencoded) and a long key.
fn encoded_batch(start_seq: u64) -> Batch {
	let inline = |value: &[u8]| Some(ValueLocation::with_inline_value(value.to_vec()).encode());
	let mut batch = Batch::new(start_seq);
	for (kind, key, value, ts) in [
		(InternalKeyKind::Set, b"alpha".to_vec(), inline(b"one"), 1),
		(InternalKeyKind::Set, b"beta".to_vec(), inline(b""), 2),
		(InternalKeyKind::Delete, b"gamma".to_vec(), None, 3),
		(InternalKeyKind::RangeDelete, b"delta".to_vec(), Some(b"epsilon".to_vec()), 4),
		(InternalKeyKind::Set, b"alpha".to_vec(), inline(&[7u8; 300]), 5),
		(InternalKeyKind::SoftDelete, b"zeta".to_vec(), None, 6),
		(InternalKeyKind::Set, vec![b'k'; 200], inline(&[9u8; 1000]), 7),
	] {
		batch.add_record(kind, key, value, ts).unwrap();
	}
	batch
}

/// Everything in `memtable`, in iteration order, as (encoded internal key, encoded value).
fn contents(memtable: &MemTable) -> Vec<(Vec<u8>, Vec<u8>)> {
	let mut iter = memtable.iter();
	let mut out = Vec::new();
	iter.seek_first().unwrap();
	while iter.valid() {
		out.push((iter.key().to_owned().encode(), iter.value_encoded().unwrap().to_vec()));
		iter.next().unwrap();
	}
	out
}

fn assert_same_entries(got: &Batch, want: &Batch, what: &str) {
	assert_eq!(got.starting_seq_num, want.starting_seq_num, "{what}: sequence");
	assert_eq!(got.entries.len(), want.entries.len(), "{what}: entries");
	for (i, (got, want)) in got.entries.iter().zip(&want.entries).enumerate() {
		assert_eq!(got.kind, want.kind, "{what}: entry {i} kind");
		assert_eq!(got.key, want.key, "{what}: entry {i} key");
		assert_eq!(got.value, want.value, "{what}: entry {i} value");
		assert_eq!(got.timestamp, want.timestamp, "{what}: entry {i} timestamp");
	}
}

#[test]
fn add_owned_leaves_the_memtable_as_add_does() {
	let by_add = MemTable::new(1 << 20);
	let by_add_owned = MemTable::new(1 << 20);
	let mut seen_keys = Vec::new();
	for start in [1u64, 100, 1000] {
		let batch = encoded_batch(start);
		seen_keys.extend(batch.entries.iter().map(|e| e.key.clone()));
		by_add.add(&batch).unwrap();

		let mut owned = batch.clone();
		let needed = owned.memtable_size_estimate();
		by_add_owned.add_owned(&mut owned, needed).unwrap();

		// The batch is spent: what it held is in the memtable now.
		assert_eq!(owned.count(), batch.count(), "an applied batch still counts its entries");
		assert!(owned.entries.iter().all(|e| e.key.is_empty() && e.value.is_none()));
	}

	assert_eq!(by_add_owned.size(), by_add.size(), "arena bytes used");
	assert_eq!(by_add_owned.lsn(), by_add.lsn(), "latest sequence number");
	assert_eq!(by_add_owned.has_range_deletions(), by_add.has_range_deletions());
	assert!(by_add_owned.has_range_deletions());
	assert_eq!(
		*by_add_owned.range_deletions.read(),
		*by_add.range_deletions.read(),
		"range deletions"
	);
	assert_eq!(contents(&by_add_owned), contents(&by_add), "iteration");
	assert_eq!(contents(&by_add_owned).len(), 3 * 7);

	seen_keys.push(b"never written".to_vec());
	for key in &seen_keys {
		for snapshot in [None, Some(0), Some(1), Some(3), Some(100), Some(104), Some(5000)] {
			assert_eq!(
				by_add_owned.get(key, snapshot),
				by_add.get(key, snapshot),
				"get({:?}, {snapshot:?})",
				String::from_utf8_lossy(key)
			);
		}
	}

	// The reservation was given back: all that is left of the arena can still be claimed.
	let left = (by_add_owned.arena_capacity() - by_add_owned.size()) as u64;
	assert!(by_add_owned.can_fit(left));
}

#[test]
fn add_owned_leaves_a_batch_that_does_not_fit_untouched() {
	// Nothing fits.
	let memtable = MemTable::new(2 * 1024);
	let mut batch = Batch::new(1);
	batch.add_record(InternalKeyKind::RangeDelete, b"a".to_vec(), Some(b"b".to_vec()), 0).unwrap();
	batch.set(vec![0u8; 8], vec![1u8; 4 * 1024], 0).unwrap();
	let before = batch.clone();
	let size = memtable.size();
	let needed = batch.memtable_size_estimate();
	let result = memtable.add_owned(&mut batch, needed);
	assert!(matches!(result, Err(Error::ArenaFull)), "got {result:?}");
	assert_same_entries(&batch, &before, "empty memtable");
	assert_eq!(memtable.size(), size);
	assert!(memtable.is_empty());
	assert_eq!(memtable.lsn(), 0);
	assert!(!memtable.has_range_deletions() && memtable.range_deletions.read().is_empty());

	// Something fits, and the batch that does not is refused whole.
	let memtable = MemTable::new(64 * 1024);
	let mut prefill = encoded_batch(1);
	memtable.add_owned(&mut prefill, encoded_batch(1).memtable_size_estimate()).unwrap();
	let (size, lsn, kept) = (memtable.size(), memtable.lsn(), contents(&memtable));
	let ranges = memtable.range_deletions.read().clone();

	let mut big = Batch::new(100);
	big.add_record(InternalKeyKind::RangeDelete, b"m".to_vec(), Some(b"n".to_vec()), 0).unwrap();
	for i in 0..200u16 {
		big.set(i.to_le_bytes().to_vec(), vec![0u8; 200], 0).unwrap();
	}
	let before = big.clone();
	let needed = big.memtable_size_estimate();
	let result = memtable.add_owned(&mut big, needed);
	assert!(matches!(result, Err(Error::ArenaFull)), "got {result:?}");
	assert_same_entries(&big, &before, "memtable with entries");
	assert_eq!(memtable.size(), size);
	assert_eq!(memtable.lsn(), lsn);
	assert_eq!(contents(&memtable), kept);
	assert_eq!(*memtable.range_deletions.read(), ranges);
	assert!(memtable.can_fit((memtable.arena_capacity() - memtable.size()) as u64));
}

/// `add_owned` trusts the size it is given. If that size was too small and the arena runs out
/// half way, it is a hard error and not `ArenaFull`, which the pipeline would answer by
/// applying the same batch again: the entries it already took would then be inserted as empty
/// keys. The entry that failed is put back, and the ones after it were never touched.
#[test]
fn add_owned_reports_a_drifting_estimate_as_an_error_that_is_not_arena_full() {
	let memtable = MemTable::new(4 * 1024);
	let inline = |value: &[u8]| Some(ValueLocation::with_inline_value(value.to_vec()).encode());
	let mut batch = Batch::new(1);
	for i in 0..400u32 {
		let key = format!("key{i:05}").into_bytes();
		if i % 3 == 2 {
			batch.delete(key, u64::from(i)).unwrap();
		} else {
			batch.add_record(InternalKeyKind::Set, key, inline(b"value"), u64::from(i)).unwrap();
		}
	}
	let before = batch.clone();

	// A size that says everything fits, where it does not.
	let result = memtable.add_owned(&mut batch, 100);
	let Err(Error::Other(message)) = result else {
		panic!("expected Error::Other, got {result:?}");
	};
	assert!(message.contains("drift"), "{message}");

	let failed =
		batch.entries.iter().position(|e| !e.key.is_empty()).expect("an entry was put back");
	assert!(failed > 0 && failed < 400, "the arena ran out part of the way: {failed}");
	for (i, (got, want)) in batch.entries.iter().zip(&before.entries).enumerate() {
		if i < failed {
			assert!(got.key.is_empty() && got.value.is_none(), "entry {i} was taken");
		} else {
			assert_eq!(got.key, want.key, "entry {i} key");
			assert_eq!(got.value, want.value, "entry {i} value");
			assert_eq!(got.kind, want.kind, "entry {i} kind");
		}
	}
	assert_eq!(memtable.lsn(), 0, "the sequence number is only published for a whole batch");
	assert_eq!(contents(&memtable).len(), failed);
	// The reservation is given back all the same.
	assert!(memtable.can_fit((memtable.arena_capacity() - memtable.size()) as u64));
}
