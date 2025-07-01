use tempdir::TempDir;

use crate::batch::Batch;
use crate::memtable::MemTable;
use crate::wal::cleanup::cleanup_old_segments;
use crate::wal::reader::Reader;
use crate::wal::recovery::replay_wal;
use crate::wal::segment::list_segment_ids;
use crate::wal::segment::{
    MultiSegmentReader, Options, Segment, SegmentRef, WAL_RECORD_HEADER_SIZE,
};
use crate::wal::writer::Wal;
use std::sync::Arc;

fn create_temp_directory() -> TempDir {
    TempDir::new("test").unwrap()
}

// Utility function to create a temporary WAL directory
fn create_test_wal_dir() -> TempDir {
    TempDir::new("wal_test").unwrap()
}

#[test]
fn test_cleanup_old_segments() {
    let temp_dir = create_test_wal_dir();

    // Create WAL with manual rotation to create multiple segments
    let opts = Options::default();
    let mut wal = Wal::open(temp_dir.path(), opts).unwrap();

    // Create 5 segments with manual rotation
    for segment in 0..5 {
        // Write records to current segment
        for i in 0..10 {
            let data = format!("seg{}_record_{:02}", segment, i).into_bytes();
            wal.append(&data).unwrap();
        }

        // Rotate to next segment (except for the last one)
        if segment < 4 {
            wal.rotate().unwrap();
        }
    }

    // Get segment IDs and verify we have at least 5
    let segment_ids_before = list_segment_ids(temp_dir.path()).unwrap();
    assert!(
        segment_ids_before.len() >= 5,
        "Expected at least 5 segments, got {}",
        segment_ids_before.len()
    );

    // Run cleanup - should remove all except the latest segment
    let removed_count = cleanup_old_segments(temp_dir.path()).unwrap();

    // Verify at least 4 segments were removed (keeping only the latest)
    assert!(
        removed_count >= 4,
        "Expected at least 4 segments to be removed, got {}",
        removed_count
    );

    // Verify only the latest segment remains
    let remaining_segment_ids = list_segment_ids(temp_dir.path()).unwrap();

    // Should have exactly 1 segment remaining
    assert_eq!(
        remaining_segment_ids.len(),
        1,
        "Expected exactly 1 segment remaining, got {}",
        remaining_segment_ids.len()
    );

    // The remaining segment should be the latest one
    let latest_segment_id = segment_ids_before.iter().max().unwrap();
    assert_eq!(
        remaining_segment_ids[0], *latest_segment_id,
        "Expected latest segment {} to remain, but got {}",
        latest_segment_id, remaining_segment_ids[0]
    );
}

#[test]
fn test_wal_replay_latest_segment_only() {
    let temp_dir = create_test_wal_dir();

    // Create WAL with manual rotation to create multiple segments
    let opts = Options::default();
    let mut wal = Wal::open(temp_dir.path(), opts).unwrap();

    // Write 10 records to first segment
    for i in 0..10 {
        let data = format!("test_record_{:02}", i).into_bytes();
        wal.append(&data).unwrap();
    }

    // Manually rotate to create second segment
    wal.rotate().unwrap();

    // Write 10 records to second segment
    for i in 10..20 {
        let data = format!("test_record_{:02}", i).into_bytes();
        wal.append(&data).unwrap();
    }

    // Manually rotate to create third segment
    wal.rotate().unwrap();

    // Write batch records to third segment (latest)
    let mut batch = Batch::new();
    for i in 20..25 {
        let key = format!("key_{:02}", i);
        let value = format!("value_{:02}", i);
        batch.set(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Encode the batch with sequence number 100
    let encoded_batch = batch.encode(100).unwrap();
    wal.append(&encoded_batch).unwrap();

    // Close WAL to ensure everything is flushed
    drop(wal);

    // Get segment IDs and verify we have at least 3
    let segment_ids = list_segment_ids(temp_dir.path()).unwrap();
    assert!(
        segment_ids.len() >= 3,
        "Expected at least 3 segments, got {}",
        segment_ids.len()
    );

    // Create a memtable for recovery
    let memtable = Arc::new(MemTable::default());

    // Replay WAL - should only replay the latest segment
    let (sequence_number, corruption_info) = replay_wal(temp_dir.path(), &memtable).unwrap();

    // Count actual entries in memtable
    let entry_count = memtable.iter().count();

    // Verify no corruption was detected
    assert!(
        corruption_info.is_none(),
        "Expected no corruption, but got: {:?}",
        corruption_info
    );

    // Verify sequence number is from the latest segment (100)
    assert_eq!(sequence_number, 100);

    // Verify the memtable contains only the data from the latest segment
    assert_eq!(entry_count, 5, "Expected 5 entries from latest segment");
}

#[test]
fn test_wal_with_zero_padding_eof_handling() {
    let temp_dir = create_temp_directory();

    // Create a WAL segment with small data that doesn't fill the block
    let opts = Options::default();
    let mut segment = Segment::<WAL_RECORD_HEADER_SIZE>::open(temp_dir.path(), 0, &opts)
        .expect("should create segment");

    // Write a small record that won't fill the entire block
    let small_data = b"hello";
    segment.append(small_data).expect("should append data");

    // Close the segment to ensure it's flushed with zero padding
    segment.close().expect("should close segment");

    // Now try to read from the segment using the Reader
    let segments =
        SegmentRef::read_segments_from_directory(temp_dir.path()).expect("should read segments");

    let multi_reader =
        MultiSegmentReader::new(segments).expect("should create multi segment reader");

    let mut reader = Reader::new(multi_reader);

    let result = reader.read();
    match result {
        Ok((data, _offset)) => {
            assert_eq!(data, small_data);
        }
        Err(e) => {
            panic!("Failed to read data: {}", e);
        }
    }

    // Try to read again
    let result2 = reader.read();
    match result2 {
        Ok(_) => {
            panic!("Expected EOF, but got data");
        }
        Err(e) => {
            match e {
                crate::wal::segment::Error::IO(io_err) => {
                    if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                        // Expected EOF error
                    } else {
                        panic!("Got unexpected IO error: {}", io_err);
                    }
                }
                crate::wal::segment::Error::Corruption(_) => {
                    panic!("Got corruption error when expecting EOF: {}", e);
                }
                _ => {
                    panic!("Got unexpected error type: {}", e);
                }
            }
        }
    }
}

#[test]
fn test_empty_wal_segment() {
    let temp_dir = create_temp_directory();

    // Create an empty WAL segment
    let opts = Options::default();
    let segment = Segment::<WAL_RECORD_HEADER_SIZE>::open(temp_dir.path(), 0, &opts)
        .expect("should create segment");

    // Close immediately without writing anything
    drop(segment);

    // Try to read from the empty segment
    let segments =
        SegmentRef::read_segments_from_directory(temp_dir.path()).expect("should read segments");

    let multi_reader =
        MultiSegmentReader::new(segments).expect("should create multi segment reader");

    let mut reader = Reader::new(multi_reader);

    let result = reader.read();
    match result {
        Ok(_) => {
            panic!("Expected EOF from empty segment, but got data");
        }
        Err(e) => {
            match e {
                crate::wal::segment::Error::IO(io_err) => {
                    if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                        // Expected EOF error from empty segment
                    } else {
                        panic!("Got unexpected IO error from empty segment: {}", io_err);
                    }
                }
                crate::wal::segment::Error::Corruption(_) => {
                    panic!(
                        "Got corruption error from empty segment when expecting EOF: {}",
                        e
                    );
                }
                _ => {
                    panic!("Got unexpected error type from empty segment: {}", e);
                }
            }
        }
    }
}
