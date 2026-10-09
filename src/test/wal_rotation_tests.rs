//! Rotating the WAL.
//!
//! The memtable tags and the segment a record is logged in are both read from the active log
//! number, so a rotation must only move it once the new segment exists.

use tempdir::TempDir;
use test_log::test;

use crate::wal::manager::Wal;
use crate::wal::{self, segment_name};

// ---------------------------------------------------------------------------
// failure handling
// ---------------------------------------------------------------------------

/// A failed rotation must leave the WAL as it was: the next segment's number is
/// taken only once its file exists, or the WAL number runs ahead of the writer and
/// of every memtable tag.
#[test]
fn wal_rotate_failure_leaves_number_and_writer_consistent() {
	let dir = TempDir::new("wal_rotation").unwrap();
	let mut wal = Wal::open(dir.path(), wal::Options::default()).unwrap();
	wal.append(b"before").unwrap();
	let n = wal.get_active_log_number();

	// An obstacle where the next segment file has to be created.
	let next = dir.path().join(segment_name(n + 1, "wal"));
	std::fs::create_dir(&next).unwrap();
	assert!(wal.rotate().is_err(), "rotation cannot create the next segment");
	assert_eq!(wal.get_active_log_number(), n, "a failed rotation leaves the number alone");

	// The writer is still on segment n and still works.
	wal.append(b"after").unwrap();
	wal.sync().unwrap();
	let current = std::fs::read(dir.path().join(segment_name(n, "wal"))).unwrap();
	assert!(current.windows(5).any(|w| w == b"after"), "the append landed in segment {n}");

	// Once the obstacle is gone the rotation succeeds and moves exactly one on.
	std::fs::remove_dir(&next).unwrap();
	assert_eq!(wal.rotate().unwrap(), n + 1);
	assert_eq!(wal.get_active_log_number(), n + 1);
	wal.append(b"later").unwrap();
	wal.sync().unwrap();
	let rotated = std::fs::read(dir.path().join(segment_name(n + 1, "wal"))).unwrap();
	assert!(rotated.windows(5).any(|w| w == b"later"));
	assert!(!rotated.windows(5).any(|w| w == b"after"));
}
