//! The optional timestamp of a `ValueLocation`: `meta | version | timestamp? | value`.
//!
//! The risk of adding a field in the middle of an encoding is a reader that still slices at the
//! old offset. The one that matters most computes the value-log garbage-collection floor from the
//! pointer inside a value (`oldest_vlog_file_id`); `ValuePointer::decode` insists on exactly 25
//! bytes, and the table writer ignores a pointer that fails to decode, so a misaligned slice would
//! not fail, it would silently lower no floor and let a live value-log file be deleted.

use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use crate::memtable::maybe_separate_to_vlog;
use crate::sstable::table::{Table, TableWriter};
use crate::vlog::{
	VLog,
	ValueLocation,
	ValuePointer,
	BIT_HAS_TIMESTAMP,
	BIT_VALUE_POINTER,
	VALUE_POINTER_SIZE,
};
use crate::{Error, InternalKey, InternalKeyKind, Options};

fn pointer(file_id: u32) -> ValuePointer {
	ValuePointer::new(file_id, 4096, 24, 100, 0xDEAD_BEEF)
}

#[test]
fn a_value_without_a_timestamp_encodes_exactly_as_it_always_did() {
	let inline = ValueLocation::with_inline_value(b"abc".to_vec()).encode();
	assert_eq!(inline, [0, 1, b'a', b'b', b'c']);

	let encoded = ValueLocation::with_pointer(pointer(7)).encode();
	assert_eq!(encoded.len(), 2 + VALUE_POINTER_SIZE);
	assert_eq!(&encoded[..2], &[BIT_VALUE_POINTER, 1]);
}

#[test]
fn a_timestamp_sits_between_the_version_and_the_value() {
	let encoded = ValueLocation::with_inline_value(b"abc".to_vec())
		.with_timestamp(Some(0x0102_0304_0506_0708))
		.encode();
	assert_eq!(encoded, [BIT_HAS_TIMESTAMP, 1, 1, 2, 3, 4, 5, 6, 7, 8, b'a', b'b', b'c']);
	assert_eq!(ValueLocation::peek_timestamp(&encoded), Some(0x0102_0304_0506_0708));
	assert_eq!(ValueLocation::peek_timestamp(&[0, 1, b'a']), None);
}

#[test]
fn every_combination_round_trips() {
	for timestamp in [None, Some(0), Some(1), Some(1_791_669_450_050_800_980), Some(u64::MAX)] {
		let inline =
			ValueLocation::with_inline_value(b"payload".to_vec()).with_timestamp(timestamp);
		let decoded = ValueLocation::decode(&inline.encode()).unwrap();
		assert_eq!(decoded, inline);
		assert!(!decoded.is_value_pointer());
		assert_eq!(decoded.timestamp, timestamp);

		let pointed = ValueLocation::with_pointer(pointer(3)).with_timestamp(timestamp);
		let decoded = ValueLocation::decode(&pointed.encode()).unwrap();
		assert_eq!(decoded, pointed);
		assert!(decoded.is_value_pointer());
		assert_eq!(ValuePointer::decode(&decoded.value).unwrap(), pointer(3));

		// An empty value, which is what a tombstone that keeps its time looks like.
		let empty = ValueLocation::with_inline_value(Vec::new()).with_timestamp(timestamp);
		assert_eq!(ValueLocation::decode(&empty.encode()).unwrap(), empty);
	}
}

#[test]
fn the_pointer_payload_is_found_with_and_without_a_timestamp() {
	let expected = pointer(9).encode();
	for timestamp in [None, Some(42)] {
		let encoded = ValueLocation::with_pointer(pointer(9)).with_timestamp(timestamp).encode();
		let payload = ValueLocation::peek_pointer_payload(&encoded).expect("a pointer");
		assert_eq!(payload, expected.as_slice(), "timestamp {timestamp:?}");
		assert_eq!(ValuePointer::decode(payload).unwrap(), pointer(9));

		let inline = ValueLocation::with_inline_value(vec![7u8; 40]).with_timestamp(timestamp);
		assert!(ValueLocation::peek_pointer_payload(&inline.encode()).is_none());
	}
}

#[test]
fn a_timestamp_flag_without_room_for_the_timestamp_is_corruption() {
	let err = ValueLocation::decode(&[BIT_HAS_TIMESTAMP, 1, 1, 2, 3]).err().unwrap();
	assert!(matches!(err, Error::Corruption(_)), "got {err:?}");
	assert_eq!(ValueLocation::peek_timestamp(&[BIT_HAS_TIMESTAMP, 1, 1, 2, 3]), None);
}

/// The floor the garbage collector deletes below. Stamped pointers must lower it exactly as
/// unstamped ones do.
#[test]
fn the_table_writer_floors_the_value_log_by_stamped_and_unstamped_pointers() {
	let dir = TempDir::new().unwrap();
	let opts = Arc::new(Options::new());

	for (name, values, expected_floor) in [
		("unstamped", [(7u32, None), (11, None)], 7u64),
		("stamped", [(7, Some(10u64)), (11, Some(11))], 7),
		("stamped lowest last", [(11, Some(10)), (7, Some(11))], 7),
		("mixed", [(11, None), (7, Some(11))], 7),
	] {
		let path = dir.path().join(format!("{name}.sst"));
		let file = std::fs::File::create(&path).unwrap();
		let mut writer = TableWriter::new(file, 1, Arc::clone(&opts), 1);
		for (i, (file_id, timestamp)) in values.iter().enumerate() {
			let value =
				ValueLocation::with_pointer(pointer(*file_id)).with_timestamp(*timestamp).encode();
			let key = InternalKey::new(
				format!("key-{i}").into_bytes(),
				10 + i as u64,
				InternalKeyKind::Set,
			);
			writer.add(key, &value).unwrap();
		}
		let size = writer.finish().unwrap();

		let table = Table::new(
			1,
			Arc::clone(&opts),
			Arc::new(std::fs::File::open(&path).unwrap()),
			size as u64,
		)
		.unwrap();
		assert_eq!(table.meta.properties.oldest_vlog_file_id, expected_floor, "{name}");
	}
}

/// A value that crosses the threshold at flush moves to the value log, and its timestamp stays
/// with the pointer that replaces it.
#[test]
fn separating_a_value_to_the_value_log_keeps_its_timestamp() {
	let dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		enable_vlog: true,
		..Default::default()
	});
	crate::test::tfs::create_dir_all(&opts, opts.vlog_dir()).unwrap();
	let vlog = Arc::new(VLog::new(Arc::clone(&opts)).unwrap());
	let key = InternalKey::new(b"key".to_vec(), 5, InternalKeyKind::Set);

	for timestamp in [None, Some(1_700_000_000_000_000_000u64)] {
		let big = vec![0xABu8; 5000];
		let encoded =
			ValueLocation::with_inline_value(big.clone()).with_timestamp(timestamp).encode();
		let separated = maybe_separate_to_vlog(&encoded, &key, Some(&vlog), 1024).unwrap();
		let location = ValueLocation::decode(&separated).unwrap();
		assert!(location.is_value_pointer(), "{timestamp:?}");
		assert_eq!(location.timestamp, timestamp);
		// The value log buffers appends; a read sees them once they are flushed.
		vlog.sync().unwrap();
		assert_eq!(location.resolve_value(Some(&vlog)).unwrap(), big);

		// A small value stays inline and keeps its bytes as they are.
		let small =
			ValueLocation::with_inline_value(b"tiny".to_vec()).with_timestamp(timestamp).encode();
		assert_eq!(maybe_separate_to_vlog(&small, &key, Some(&vlog), 1024).unwrap(), small);
	}
}

/// The value of a range delete is the end key of the range, whatever its bytes are. They must pass
/// through a flush and a table untouched and must never be taken for a pointer into the value log,
/// even when they look like a value location with a timestamp or a pointer.
#[test]
fn the_end_key_of_a_range_delete_is_never_read_as_a_value_location() {
	let dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		enable_vlog: true,
		..Default::default()
	});
	crate::test::tfs::create_dir_all(&opts, opts.vlog_dir()).unwrap();
	let vlog = Arc::new(VLog::new(Arc::clone(&opts)).unwrap());
	let key = InternalKey::new(b"start".to_vec(), 5, InternalKeyKind::RangeDelete);

	// Every first byte, every length that matters: too short for a timestamp, and exactly the
	// length of a pointer behind a timestamp or behind the bare header.
	for first in 0..=255u8 {
		for len in [1usize, 2, 9, 10, 11, 27, 35, 36, 64] {
			let mut end = vec![0x41u8; len];
			end[0] = first;
			let flushed = maybe_separate_to_vlog(&end, &key, Some(&vlog), 0).unwrap();
			assert_eq!(flushed, end, "first byte {first:#04x}, {len} bytes");
		}
	}

	// And in a table: a range delete whose end key has the layout of a stamped pointer to file 1
	// does not lower the value-log floor of the table, which holds a pointer to file 9.
	let mut end = vec![BIT_VALUE_POINTER | BIT_HAS_TIMESTAMP, 1];
	end.extend_from_slice(&[0u8; 8]);
	end.extend_from_slice(&pointer(1).encode());
	let path = dir.path().join("range.sst");
	let file = std::fs::File::create(&path).unwrap();
	let mut writer = TableWriter::new(file, 1, Arc::clone(&opts), 1);
	writer.add(InternalKey::new(b"a".to_vec(), 7, InternalKeyKind::RangeDelete), &end).unwrap();
	writer
		.add(
			InternalKey::new(b"b".to_vec(), 6, InternalKeyKind::Set),
			&ValueLocation::with_pointer(pointer(9)).encode(),
		)
		.unwrap();
	let size = writer.finish().unwrap();
	let table = Table::new(
		1,
		Arc::clone(&opts),
		Arc::new(std::fs::File::open(&path).unwrap()),
		size as u64,
	)
	.unwrap();
	assert_eq!(table.meta.properties.oldest_vlog_file_id, 9);
}

/// A version without a time reads as written when the versions before it were.
#[test]
fn a_version_without_a_time_takes_the_newest_time_of_the_versions_before_it() {
	// Newest write first: (stamp, is a barrier).
	let times = ValueLocation::infer_times(&[
		(None, false),
		(Some(300), false),
		(None, false),
		(Some(100), false),
		(Some(200), false),
		(None, false),
	]);
	assert_eq!(times, vec![300, 300, 200, 100, 200, 0]);

	// Nothing below a barrier counts.
	let times = ValueLocation::infer_times(&[
		(None, false),
		(None, true),
		(Some(500), false),
		(Some(900), false),
	]);
	assert_eq!(times, vec![0, 0, 500, 900]);

	// A replace is a version: its own time counts, and what is below it does not.
	let times = ValueLocation::infer_times(&[(None, false), (Some(50), true), (Some(900), false)]);
	assert_eq!(times, vec![50, 50, 900]);

	assert_eq!(ValueLocation::infer_times(&[]), Vec::<u64>::new());
}
