//! Tests for what happens when a WAL record cannot be decoded, and when it
//! cannot be written.
//!
//! * `Batch::decode` runs on every record at recovery. It must return an error for a malformed
//!   record instead of panicking or reading past the end, and it must consume the whole record: a
//!   record with bytes left over would silently drop whatever is in them.
//! * A WAL append that fails part-way can leave the start of a record in the segment. Recovery
//!   stops at the first damaged record, so every record acknowledged after it would be dropped. A
//!   failed append must cut the segment back to the last complete record and refuse further appends
//!   until the writer is replaced by a rotation.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use crate::batch::{Batch, BATCH_VERSION};
use crate::error::Error;
use crate::varint::put_varint_u64;
use crate::vlog::ValuePointer;
use crate::wal::manager::Wal;
use crate::wal::parallel_recovery::decode_segment_batches;
use crate::wal::recovery::replay_wal;
use crate::wal::{segment_name, Options as WalOptions};
use crate::{InternalKeyKind, Options, Tree, WalRecoveryMode};

// ---------------------------------------------------------------------------
// Batch::decode
// ---------------------------------------------------------------------------

fn entry(kind: InternalKeyKind, key: Vec<u8>, value: Option<Vec<u8>>, ts: u64) -> Batch {
	let mut batch = Batch::new(7);
	batch.add_record(kind, key, value, ts).unwrap();
	batch
}

/// Every shape of valid batch the encoder can produce.
fn valid_batches() -> Vec<(&'static str, Batch)> {
	let mut shapes = Vec::new();

	for seq in [0u64, 1, 127, 128, 16_384, u32::MAX as u64 + 1, u64::MAX >> 8] {
		shapes.push(("empty batch", Batch::new(seq)));
	}

	shapes.push((
		"one set",
		entry(InternalKeyKind::Set, b"key".to_vec(), Some(b"value".to_vec()), 1),
	));
	shapes.push(("one delete", entry(InternalKeyKind::Delete, b"key".to_vec(), None, 2)));

	// One entry of every kind the format allows.
	let mut kinds = Batch::new(1000);
	for (i, kind) in [
		InternalKeyKind::Delete,
		InternalKeyKind::SoftDelete,
		InternalKeyKind::Set,
		InternalKeyKind::Merge,
		InternalKeyKind::LogData,
		InternalKeyKind::RangeDelete,
		InternalKeyKind::Replace,
		InternalKeyKind::Separator,
		InternalKeyKind::Max,
	]
	.into_iter()
	.enumerate()
	{
		kinds
			.add_record(
				kind,
				format!("k{i}").into_bytes(),
				Some(vec![i as u8 + 1; i + 1]),
				i as u64,
			)
			.unwrap();
	}
	shapes.push(("every kind", kinds));

	// Key, value and timestamp sizes on both sides of each varint width.
	for len in [0usize, 1, 127, 128, 16_383, 16_384] {
		shapes.push((
			"key length",
			entry(InternalKeyKind::Set, vec![b'k'; len], Some(b"v".to_vec()), 0),
		));
	}
	for len in [1usize, 127, 128, 16_383, 16_384, 70_000] {
		shapes.push((
			"value length",
			entry(InternalKeyKind::Set, b"k".to_vec(), Some(vec![b'v'; len]), 0),
		));
	}
	for ts in [0u64, 127, 128, 1 << 35, u64::MAX] {
		shapes.push((
			"timestamp",
			entry(InternalKeyKind::Set, b"k".to_vec(), Some(b"v".to_vec()), ts),
		));
	}

	let mut unicode = Batch::new(5);
	unicode.add_record(InternalKeyKind::Set, "🔑".into(), Some("🗝️".into()), 1).unwrap();
	shapes.push(("unicode", unicode));

	// Some entries carry a value pointer, some do not.
	let mut pointers = Batch::new(50);
	for i in 0..6u32 {
		pointers
			.add_record(InternalKeyKind::Set, format!("p{i}").into_bytes(), None, u64::from(i))
			.unwrap();
		if i % 2 == 0 {
			pointers.valueptrs[i as usize] =
				Some(ValuePointer::new(i, u64::from(i) * 1000, 8, 4096 + i, 0xdead_beef));
		}
	}
	shapes.push(("value pointers", pointers));

	let mut many = Batch::new(1);
	for i in 0..1000u32 {
		many.add_record(
			InternalKeyKind::Set,
			i.to_be_bytes().to_vec(),
			Some(vec![(i % 251) as u8 + 1; (i % 40) as usize + 1]),
			u64::from(i),
		)
		.unwrap();
	}
	shapes.push(("1000 entries", many));

	shapes
}

fn assert_same(shape: &str, decoded: &Batch, original: &Batch) {
	assert_eq!(decoded.version, original.version, "{shape}");
	assert_eq!(decoded.starting_seq_num, original.starting_seq_num, "{shape}");
	assert_eq!(decoded.entries.len(), original.entries.len(), "{shape}");
	for (i, (a, b)) in decoded.entries.iter().zip(&original.entries).enumerate() {
		assert_eq!(a.kind, b.kind, "{shape} entry {i}");
		assert_eq!(a.key, b.key, "{shape} entry {i}");
		assert_eq!(a.value, b.value, "{shape} entry {i}");
		assert_eq!(a.timestamp, b.timestamp, "{shape} entry {i}");
	}
	let pointers = |batch: &Batch| -> Vec<Option<Vec<u8>>> {
		batch.valueptrs.iter().map(|p| p.as_ref().map(|p| p.encode())).collect()
	};
	assert_eq!(pointers(decoded), pointers(original), "{shape}");
}

/// Decodes `data`, reporting a panic as a failure of the input rather than of
/// the whole test.
fn decode(data: &[u8]) -> std::thread::Result<crate::Result<Batch>> {
	std::panic::catch_unwind(|| Batch::decode(data))
}

#[test]
fn every_valid_batch_shape_round_trips() {
	for (shape, batch) in valid_batches() {
		let encoded = batch.encode().unwrap();
		let decoded = Batch::decode(&encoded)
			.unwrap_or_else(|e| panic!("{shape}: a valid batch must decode: {e}"));
		assert_same(shape, &decoded, &batch);
		assert_eq!(
			decoded.encode().unwrap(),
			encoded,
			"{shape}: re-encoding must be byte-identical"
		);
	}
}

/// Every length for a small record. For a large one, the first and last 64
/// bytes and a stride in between, since a 70 KB value would otherwise be
/// decoded tens of thousands of times for the same answer.
fn truncation_points(len: usize) -> Vec<usize> {
	if len <= 2048 {
		return (0..len).collect();
	}
	(0..64).chain((64..len - 64).step_by(509)).chain(len - 64..len).collect()
}

/// A record cut off anywhere is not a batch.
#[test]
fn every_truncation_of_a_valid_batch_is_rejected() {
	let mut checked = 0;
	let mut bad = Vec::new();
	for (shape, batch) in valid_batches() {
		let encoded = batch.encode().unwrap();
		for len in truncation_points(encoded.len()) {
			checked += 1;
			match decode(&encoded[..len]) {
				Ok(Err(Error::InvalidBatchRecord)) => {}
				Ok(other) => bad.push(format!("{shape}, {len}/{} bytes: {other:?}", encoded.len())),
				Err(_) => bad.push(format!("{shape}, {len}/{} bytes: panicked", encoded.len())),
			}
		}
	}
	assert!(checked > 1000, "only {checked} truncations were tried");
	assert!(
		bad.is_empty(),
		"{} of {checked} truncations were not rejected: {:#?}",
		bad.len(),
		&bad[..bad.len().min(10)]
	);
}

#[test]
fn trailing_bytes_are_rejected() {
	let batch = entry(InternalKeyKind::Set, b"key".to_vec(), Some(b"value".to_vec()), 1);
	let encoded = batch.encode().unwrap();
	let other = entry(InternalKeyKind::Set, b"other".to_vec(), Some(b"value".to_vec()), 2);

	let mut one_byte = encoded.clone();
	one_byte.push(0);
	let mut many = encoded.clone();
	many.extend_from_slice(&[0xab; 200]);
	// Two batches back to back: the second would be silently dropped.
	let mut two_batches = encoded;
	two_batches.extend_from_slice(&other.encode().unwrap());

	for (what, data) in
		[("one byte", one_byte), ("200 bytes", many), ("a second batch", two_batches)]
	{
		match decode(&data) {
			Ok(Err(Error::InvalidBatchRecord)) => {}
			Ok(other) => panic!("trailing {what} accepted: {other:?}"),
			Err(_) => panic!("trailing {what} panicked"),
		}
	}
}

#[test]
fn unknown_versions_are_rejected() {
	let encoded =
		entry(InternalKeyKind::Set, b"key".to_vec(), Some(b"value".to_vec()), 1).encode().unwrap();
	for version in (0..=u8::MAX).filter(|v| *v != BATCH_VERSION) {
		let mut data = encoded.clone();
		data[0] = version;
		assert!(
			matches!(decode(&data), Ok(Err(Error::InvalidBatchRecord))),
			"version {version} was not rejected"
		);
	}
}

#[test]
fn an_unknown_kind_or_pointer_flag_is_rejected() {
	let mut batch = Batch::new(1);
	batch.add_record(InternalKeyKind::Set, b"k".to_vec(), Some(b"v".to_vec()), 0).unwrap();
	let encoded = batch.encode().unwrap();
	// version, seq (1 byte), count (1 byte), then the kind.
	let kind_at = 3;
	assert_eq!(encoded[kind_at], InternalKeyKind::Set as u8);
	let mut bad_kind = encoded.clone();
	bad_kind[kind_at] = 191;
	assert!(matches!(decode(&bad_kind), Ok(Err(Error::InvalidBatchRecord))));

	// The last byte is the value pointer flag: 0 or 1, nothing else.
	let mut bad_flag = encoded;
	*bad_flag.last_mut().unwrap() = 2;
	assert!(matches!(decode(&bad_flag), Ok(Err(Error::InvalidBatchRecord))));
}

/// Length prefixes that claim more than the record holds are rejected before
/// anything is sized from them.
#[test]
fn oversized_length_prefixes_are_rejected() {
	let header = |count: u64| {
		let mut data = vec![BATCH_VERSION];
		put_varint_u64(&mut data, 1);
		put_varint_u64(&mut data, count);
		data
	};
	let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();

	// More entries than the bytes that follow could hold.
	cases.push(("count 1M, no body", header(1_000_000)));
	cases.push(("count u32::MAX, no body", header(u64::from(u32::MAX))));
	let mut few_bytes = header(1_000_000);
	few_bytes.extend_from_slice(&[InternalKeyKind::Set as u8, 1, b'k', 1, b'v', 0]);
	cases.push(("count 1M, one entry", few_bytes));
	// A count above u32::MAX is not a count.
	cases.push(("count 2^40", header(1 << 40)));

	// A key or value length larger than the rest of the record.
	for (what, key_len, value_len) in [
		("key length past the end", 1_000u64, 0u64),
		("key length u64::MAX", u64::MAX, 0),
		("key length usize-ish", 1 << 62, 0),
		("value length past the end", 1, 1_000),
		("value length u64::MAX", 1, u64::MAX),
	] {
		let mut data = header(1);
		data.push(InternalKeyKind::Set as u8);
		put_varint_u64(&mut data, key_len);
		data.push(b'k');
		put_varint_u64(&mut data, value_len);
		data.push(b'v');
		put_varint_u64(&mut data, 0);
		data.push(0);
		cases.push((what, data));
	}

	// A varint that never ends.
	let mut unterminated = vec![BATCH_VERSION];
	unterminated.extend_from_slice(&[0xff; 20]);
	cases.push(("unterminated sequence number", unterminated));

	for (what, data) in cases {
		match decode(&data) {
			Ok(Err(Error::InvalidBatchRecord)) => {}
			Ok(other) => panic!("{what}: accepted: {other:?}"),
			Err(_) => panic!("{what}: panicked"),
		}
	}
}

/// xorshift64*, so the inputs are the same on every run.
struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 >> 12;
		self.0 ^= self.0 << 25;
		self.0 ^= self.0 >> 27;
		self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
	}
}

#[test]
fn garbage_never_panics() {
	let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
	let mut panicked = Vec::new();
	for round in 0..20_000 {
		let len = (rng.next() % 300) as usize;
		let mut data: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
		// Most random inputs fail on the version byte, which tests nothing past it.
		if round % 4 != 0 && !data.is_empty() {
			data[0] = BATCH_VERSION;
		}
		if decode(&data).is_err() {
			panicked.push(data);
		}
	}
	assert!(
		panicked.is_empty(),
		"{} inputs panicked, first: {:?}",
		panicked.len(),
		panicked.first()
	);
}

/// Flip every byte of a valid record to every value: the result is either a
/// batch or an error, never a panic.
#[test]
fn every_single_byte_mutation_is_handled() {
	let mut mixed = Batch::new(300);
	mixed.add_record(InternalKeyKind::Set, b"first".to_vec(), Some(b"value".to_vec()), 5).unwrap();
	mixed.add_record(InternalKeyKind::Delete, b"second".to_vec(), None, 6).unwrap();
	mixed.add_record(InternalKeyKind::Set, b"third".to_vec(), None, 7).unwrap();
	mixed.valueptrs[2] = Some(ValuePointer::new(1, 2, 3, 4, 5));
	let encoded = mixed.encode().unwrap();

	let mut panicked = Vec::new();
	for at in 0..encoded.len() {
		for value in 0..=u8::MAX {
			let mut data = encoded.clone();
			data[at] = value;
			if decode(&data).is_err() {
				panicked.push((at, value));
			}
		}
	}
	assert!(
		panicked.is_empty(),
		"{} mutations panicked, first: {:?}",
		panicked.len(),
		panicked.first()
	);
}

// ---------------------------------------------------------------------------
// recovery on records that cannot be decoded
// ---------------------------------------------------------------------------

fn wal_record(seq: u64, key: &str, value_len: usize) -> Vec<u8> {
	let mut batch = Batch::new(seq);
	batch
		.add_record(InternalKeyKind::Set, key.as_bytes().to_vec(), Some(vec![b'v'; value_len]), 0)
		.unwrap();
	batch.encode().unwrap()
}

fn replay(dir: &Path) -> crate::Result<()> {
	replay_wal(dir, 0, 1 << 20).map(|_| ())
}

/// Records that pass the WAL's own CRC but are not batches stop recovery with
/// an error. Recovery used to panic on some of them and to accept others while
/// ignoring part of the record.
#[test]
fn recovery_rejects_records_that_are_not_batches() {
	let valid = wal_record(1, "key", 8);
	let mut two_batches = valid.clone();
	two_batches.extend_from_slice(&wal_record(2, "other", 8));
	let mut trailing = valid.clone();
	trailing.push(0);
	let cases: Vec<(&str, Vec<u8>)> = vec![
		("count without entries", vec![BATCH_VERSION, 1, 5]),
		("version only", vec![BATCH_VERSION]),
		("unknown version", vec![0x7f, 1, 0]),
		("a truncated batch", valid[..valid.len() - 1].to_vec()),
		("a trailing byte", trailing),
		("two batches in one record", two_batches),
	];

	for (what, record) in cases {
		let dir = TempDir::new().unwrap();
		let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
		wal.append(&valid).unwrap();
		wal.append(&record).unwrap();
		wal.close().unwrap();

		let result = std::panic::catch_unwind(|| replay(dir.path()));
		match result {
			Ok(Err(Error::InvalidBatchRecord)) => {}
			Ok(other) => panic!("{what}: expected InvalidBatchRecord, got {other:?}"),
			Err(_) => panic!("{what}: recovery panicked"),
		}
	}
}

#[test_log::test(tokio::test)]
async fn opening_a_database_with_an_undecodable_record_is_an_error() {
	let dir = TempDir::new().unwrap();
	let o = Arc::new(Options {
		path: dir.path().to_path_buf(),
		flush_on_close: false,
		..Default::default()
	});
	let tree = Tree::new(Arc::clone(&o)).unwrap();
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	drop(tree);

	let mut wal = Wal::open(&o.wal_dir(), WalOptions::default()).unwrap();
	wal.append(&[BATCH_VERSION, 1, 9]).unwrap();
	wal.close().unwrap();

	match Tree::new(Arc::clone(&o)) {
		Err(Error::InvalidBatchRecord) => {}
		Err(e) => panic!("expected InvalidBatchRecord, got: {e}"),
		Ok(_) => panic!("a record that is not a batch must not open"),
	}
}

// ---------------------------------------------------------------------------
// a failed append
// ---------------------------------------------------------------------------

const SEGMENT: &str = "00000000000000000000.wal";

fn file_len(path: &Path) -> u64 {
	fs::metadata(path).unwrap().len()
}

/// Keys of every record in every segment, in order. Fails on a segment that
/// does not parse end to end.
fn recovered_keys(dir: &Path) -> Vec<String> {
	let mut segments: Vec<PathBuf> = fs::read_dir(dir)
		.unwrap()
		.map(|e| e.unwrap().path())
		.filter(|p| p.extension().is_some_and(|ext| ext == "wal"))
		.collect();
	segments.sort();
	let mut keys = Vec::new();
	for segment in segments {
		let id: u64 = segment.file_stem().unwrap().to_str().unwrap().parse().unwrap();
		let (_, batches) = decode_segment_batches(&segment, id)
			.unwrap_or_else(|e| panic!("{} must parse end to end: {e}", segment.display()));
		for batch in batches {
			keys.extend(batch.entries.iter().map(|e| String::from_utf8(e.key.clone()).unwrap()));
		}
	}
	keys
}

/// Fails the append of a record at every point where it can fail. After each
/// failure the segment holds exactly the acknowledged records, the writer
/// refuses further appends, and a rotation gives a working writer again.
fn append_failure_sweep(value_len: usize, min_failure_points: usize) {
	// Each pass fails one write later, so `fail_after` is the number of writes
	// that have been failed so far when the append finally succeeds.
	for fail_after in 0..64 {
		let dir = TempDir::new().unwrap();
		let segment = dir.path().join(SEGMENT);
		let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
		for i in 0..3 {
			wal.append(&wal_record(i + 1, &format!("acked{i}"), 100)).unwrap();
		}
		let acked_len = file_len(&segment);

		wal.fail_writes_after(fail_after);
		let result = wal.append(&wal_record(4, "failed", value_len));
		if result.is_ok() {
			// Past the last write of this record: the control.
			assert!(
				fail_after >= min_failure_points,
				"the record has only {fail_after} failure points, expected {min_failure_points}"
			);
			assert_eq!(recovered_keys(dir.path()).len(), 4);
			return;
		}

		let at = format!("failure after {fail_after} writes of a {value_len} byte value");
		assert_eq!(
			file_len(&segment),
			acked_len,
			"{at}: part of the record is left in the segment"
		);
		assert!(
			wal.append(&wal_record(5, "later", 100)).is_err(),
			"{at}: a poisoned writer must refuse further appends"
		);
		assert_eq!(file_len(&segment), acked_len, "{at}: a refused append wrote to the segment");

		// The segment is still sound for recovery, and a rotation replaces the writer.
		wal.rotate().unwrap();
		wal.append(&wal_record(6, "after", 100)).unwrap();
		wal.close().unwrap();
		assert_eq!(
			recovered_keys(dir.path()),
			["acked0", "acked1", "acked2", "after"],
			"{at}: recovery must see exactly the acknowledged records"
		);
	}
	panic!("the append never succeeded");
}

#[test]
fn a_failed_append_of_a_small_record_leaves_nothing_behind() {
	append_failure_sweep(64, 3);
}

#[test]
fn a_failed_append_of_a_two_block_record_leaves_nothing_behind() {
	append_failure_sweep(40_000, 6);
}

/// A 100 KB record is four fragments, so a failure on the Nth write lands
/// inside the record with earlier fragments already in the file.
#[test]
fn a_failed_append_inside_a_multi_fragment_record_leaves_nothing_behind() {
	append_failure_sweep(100_000, 10);
}

/// A record whose append failed was never acknowledged. It must not come back
/// at recovery because a later flush wrote out what was left in the buffer.
#[test]
fn a_failed_append_is_never_resurrected() {
	let dir = TempDir::new().unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	wal.append(&wal_record(1, "acked", 100)).unwrap();

	// Header and payload are buffered, and the flush at the end of the append fails.
	wal.fail_writes_after(2);
	assert!(wal.append(&wal_record(2, "failed", 100)).is_err());

	// Rotating syncs the old segment, which flushes whatever the writer still holds.
	wal.rotate().unwrap();
	wal.append(&wal_record(3, "after", 100)).unwrap();
	wal.close().unwrap();
	assert_eq!(recovered_keys(dir.path()), ["acked", "after"]);
}

/// The same through the whole tree: a commit whose WAL append fails is an
/// error, later commits fail cleanly, and a restart recovers exactly the
/// commits that were acknowledged.
#[test_log::test(tokio::test)]
async fn tree_recovers_exactly_the_acknowledged_commits_after_a_failed_append() {
	let dir = TempDir::new().unwrap();
	let o = Arc::new(Options {
		path: dir.path().to_path_buf(),
		flush_on_close: false,
		wal_recovery_mode: WalRecoveryMode::AbsoluteConsistency,
		..Default::default()
	});

	let tree = Tree::new(Arc::clone(&o)).unwrap();
	// Opening continues in a fresh segment, so the one commits go to is not segment 0.
	let active = tree.core.inner.wal.read().get_active_log_number();
	let segment = o.wal_dir().join(segment_name(active, "wal"));
	for i in 0..5 {
		let mut txn = tree.begin().unwrap();
		txn.set(format!("acked{i}").as_bytes(), &[b'x'; 64]).unwrap();
		txn.commit().await.unwrap();
	}
	let acked_len = file_len(&segment);

	// A commit large enough to span blocks, failing inside the record.
	tree.core.inner.wal.write().fail_writes_after(8);
	let mut txn = tree.begin().unwrap();
	txn.set(b"failed", vec![b'y'; 100_000]).unwrap();
	let outcome = tokio::time::timeout(Duration::from_secs(20), txn.commit()).await;
	assert!(matches!(outcome, Ok(Err(_))), "the commit must report the failed append: {outcome:?}");
	assert_eq!(file_len(&segment), acked_len, "no part of the failed record may stay");

	// The writer is poisoned: later commits fail rather than land after damage.
	let mut txn = tree.begin().unwrap();
	txn.set(b"later", &[b'z'; 64]).unwrap();
	let outcome = tokio::time::timeout(Duration::from_secs(20), txn.commit()).await;
	assert!(matches!(outcome, Ok(Err(_))), "a later commit must fail cleanly: {outcome:?}");
	assert_eq!(file_len(&segment), acked_len);

	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	drop(tree);

	// Recovery in the strictest mode: nothing to repair, nothing lost, nothing extra.
	let tree = Tree::new(Arc::clone(&o)).expect("the segment must recover without repair");
	let txn = tree.begin().unwrap();
	for i in 0..5 {
		assert_eq!(txn.get(format!("acked{i}").as_bytes()).unwrap(), Some(vec![b'x'; 64]));
	}
	assert_eq!(txn.get(b"failed").unwrap(), None);
	assert_eq!(txn.get(b"later").unwrap(), None);
}
