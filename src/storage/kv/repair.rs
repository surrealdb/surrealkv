use crate::storage::{
    kv::{
        error::{Error, Result},
        reader::{Reader, TxReader},
    },
    log::{
        aof::log::Aol, Error as LogError, IOError, MultiSegmentReader, Segment, SegmentRef,
        BLOCK_SIZE,
    },
};

/// Repairs the corrupted segment identified by `corrupted_segment_id` and ensures that
/// the records up to `corrupted_offset_marker` are appended correctly.
///
/// # Arguments
///
/// * `corrupted_segment_id` - The ID of the corrupted segment to be repaired.
/// * `corrupted_offset_marker` - The offset marker indicating the point up to which records are to be repaired.
///
/// # Returns
///
/// Returns an `Result` indicating the success of the repair operation.
pub fn repair(
    aol: &mut Aol,
    corrupted_segment_id: u64,
    corrupted_offset_marker: u64,
) -> Result<()> {
    // Read the list of segments from the directory
    let segs = SegmentRef::read_segments_from_directory(&aol.dir)?;

    // Prepare to store information about the corrupted segment
    let mut corrupted_segment_info = None;

    // Loop through the segments
    for s in &segs {
        // Close the active segment if its ID matches
        if s.id == aol.active_segment_id {
            aol.active_segment.close()?;
        }

        // Store information about the corrupted segment
        if s.id == corrupted_segment_id {
            corrupted_segment_info = Some((s.file_path.clone(), s.file_header_offset));
        }

        // Remove segments newer than the corrupted segment
        if s.id > corrupted_segment_id {
            std::fs::remove_file(&s.file_path)?;
        }
    }

    // If information about the corrupted segment is not available, return an error
    if corrupted_segment_info.is_none() {
        return Err(Error::LogError(LogError::IO(IOError::new(
            std::io::ErrorKind::Other,
            "Corrupted segment not found",
        ))));
    }

    // Retrieve the information about the corrupted segment
    let (corrupted_segment_path, corrupted_segment_file_header_offset) =
        corrupted_segment_info.unwrap();

    // Prepare the repaired segment path
    let repaired_segment_path = corrupted_segment_path.with_extension("repair");

    // Rename the corrupted segment to the repaired segment
    std::fs::rename(&corrupted_segment_path, &repaired_segment_path)?;

    // Open a new segment as the active segment
    let new_segment = Segment::open(&aol.dir, corrupted_segment_id, &aol.opts)?;
    aol.active_segment = new_segment;
    aol.active_segment_id = corrupted_segment_id;

    // Create a segment reader for the repaired segment
    let segments: Vec<SegmentRef> = vec![SegmentRef {
        file_path: repaired_segment_path.clone(),
        file_header_offset: corrupted_segment_file_header_offset,
        id: corrupted_segment_id,
    }];
    let segment_reader = MultiSegmentReader::new(segments)?;

    // Initialize a reader for the segment
    let reader = Reader::new_from(segment_reader, aol.opts.max_file_size, BLOCK_SIZE);
    let mut reader = TxReader::new(reader);

    // Read records until the offset marker is reached
    while let Ok((data, cur_offset)) = reader.read() {
        if cur_offset >= corrupted_offset_marker {
            break;
        }

        aol.append(&data)?;
    }

    // Flush and close the active segment
    aol.active_segment.close()?;

    // Remove the repaired segment file
    std::fs::remove_file(&repaired_segment_path)?;

    // Open the next segment and make it active
    aol.active_segment_id += 1;
    let new_segment = Segment::open(&aol.dir, aol.active_segment_id, &aol.opts)?;
    aol.active_segment = new_segment;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;
    use std::time::Duration;

    use super::*;

    use crate::storage::kv::entry::TxRecord;
    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::{Store, Task, TaskRunner};
    use crate::storage::kv::transaction::Durability;
    use crate::storage::log::{read_file_header, Segment, SegmentRef, WAL_RECORD_HEADER_SIZE};

    use bytes::Bytes;
    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[tokio::test]
    async fn store_repair() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_segment_size = 40;

        // Create a new store instance with VariableKey as the key type
        let store = Store::new(opts.clone()).expect("should create store");

        // Define key-value pairs for the test
        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let default_value = Bytes::from("val");

        {
            // Start a new read-write transaction (txn1)
            let mut txn1 = store.begin().unwrap();
            txn1.set_durability(Durability::Immediate);
            txn1.set(&key1, &default_value).unwrap();
            txn1.commit().await.unwrap();
        }

        {
            // Start another read-write transaction (txn2)
            let mut txn2 = store.begin().unwrap();
            txn2.set_durability(Durability::Immediate);
            txn2.set(&key2, &default_value).unwrap();
            txn2.commit().await.unwrap();
        }

        // // Drop the store to simulate closing it
        // store.close().await.unwrap();

        // // wait for 1 ms
        // tokio::time::sleep(Duration::from_millis(1)).await;

        let mut clog = store.inner.as_ref().unwrap().core.clog.write();
        // Open the segment file for reading and writing
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&clog.active_segment.file_path)
            .expect("should open");

        // Read the file header and move the cursor to the first record
        read_file_header(&mut file).expect("should read");

        // Corrupt the checksum of the second record
        let offset_to_edit = 195;
        let new_byte_value = 0x55;
        file.seek(SeekFrom::Start(offset_to_edit as u64))
            .expect("should seek");
        file.write_all(&[new_byte_value]).expect("should write");

        let mut corrupted_segment_id = 0;
        let mut corrupted_offset_marker = 0;
        // Read and repair the corrupted segment
        {
            let clog_subdir = opts.dir.join("clog");
            let sr = SegmentRef::read_segments_from_directory(&clog_subdir)
                .expect("should read segments");

            let mut reader = Reader::new_from(
                MultiSegmentReader::new(sr).expect("should create"),
                opts.max_segment_size,
                1000,
            );
            let mut tx_reader = TxReader::new(reader);
            let mut tx = TxRecord::new(opts.max_tx_entries as usize);

            let mut needs_repair = false;

            loop {
                // Reset the transaction record before reading into it.
                // Keeping the same transaction record instance avoids
                // unnecessary allocations.
                tx.reset();

                // Read the next transaction record from the log.
                match tx_reader.read_into(&mut tx) {
                    Ok(value_offsets) => value_offsets,
                    Err(e) => match e {
                        Error::LogError(LogError::Corruption(err)) => {
                            needs_repair = true;
                            corrupted_segment_id = err.segment_id;
                            corrupted_offset_marker = err.offset;
                            // assert_eq!(err.segment_id, 4);
                            // assert_eq!(err.offset, 22);

                            break;
                        }
                        err => panic!(
                            "Expected a CorruptionError, but got a different error {}",
                            err
                        ),
                    },
                };
            }
        }

        repair(&mut clog, corrupted_segment_id, corrupted_offset_marker as u64).unwrap();
        // // Repair the corrupted segment
        // let opts = Options::default().with_wal();
        // let mut a = Wal::open(temp_dir.path(), opts).expect("should create wal");
        // a.repair(corrupted_segment_id, corrupted_offset_marker)
        //     .expect("should repair");

        // // Verify the repaired segment
        // {
        //     let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
        //         .expect("should read segments");

        //     let mut reader = Reader::new(MultiSegmentReader::new(sr).expect("should create"));

        //     // Read the valid records after repair
        //     let rec = reader.read().expect("should read");
        //     assert_eq!(rec.0, vec![1, 2, 3, 4]);
        //     assert_eq!(reader.total_read, 11);

        //     // Ensure no further records can be read
        //     reader.read().expect_err("should not read");
        // }

        // // Append new data to the repaired segment
        // let r = a.append(&[4, 5, 6, 7, 8, 9, 10]);
        // assert!(r.is_ok());
        // assert_eq!(14, r.unwrap().1);
        // assert!(a.close().is_ok());

        // // Verify the appended data
        // {
        //     let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
        //         .expect("should read segments");

        //     let mut reader = Reader::new(MultiSegmentReader::new(sr).expect("should create"));

        //     // Read the valid records after append
        //     let rec = reader.read().expect("should read");
        //     assert_eq!(rec.0, vec![1, 2, 3, 4]);
        //     assert_eq!(reader.total_read, 11);

        //     let rec = reader.read().expect("should read");
        //     assert_eq!(rec.0, vec![4, 5, 6, 7, 8, 9, 10]);
        //     assert_eq!(reader.total_read, BLOCK_SIZE + 14);
        // }
    }
}
