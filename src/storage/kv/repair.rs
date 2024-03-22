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
    max_tx_entries: usize,
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

    let mut count = 0;
    // Read records until the offset marker is reached
    while let Ok((data, cur_offset)) = reader.read(max_tx_entries) {
        if cur_offset >= corrupted_offset_marker {
            break;
        }

        aol.append(data)?;
        count += 1;
    }

    // Flush and close the active segment
    aol.active_segment.close()?;

    // Remove the repaired segment file
    std::fs::remove_file(&repaired_segment_path)?;

    // Open the next segment and make it active
    if count > 0 {
        aol.active_segment_id += 1;
    }
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
    use std::path::Path;

    use super::*;

    use crate::storage::kv::entry::TxRecord;
    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::Store;
    use crate::storage::kv::transaction::Durability;
    use crate::storage::log::{read_file_header, SegmentRef};

    use bytes::Bytes;
    use tempdir::TempDir;

    const MAX_TX_ENTRIES: usize = 10;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    async fn setup_store_with_data(opts: Options, keys: Vec<Bytes>, value: Bytes) -> Store {
        let store = Store::new(opts.clone()).expect("should create store");
        append_data(&store, keys, &value).await;

        store
    }

    async fn append_data(store: &Store, keys: Vec<Bytes>, val: &Bytes) {
        for key in keys {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.set_durability(Durability::Immediate);
            txn.set(&key, val).unwrap();
            txn.commit().await.unwrap();
        }
    }

    async fn corrupt_and_repair(store: &Store, opts: Options, segment_num: usize, corruption_offset: u64) {
        let mut clog = store.inner.as_ref().unwrap().core.clog.write();

        let clog_subdir = opts.dir.join("clog");
        let sr =
            SegmentRef::read_segments_from_directory(&clog_subdir).expect("should read segments");

        // Open the nth segment file for corrupting
        let file_path = &sr[segment_num-1].file_path;
        corrupt_segment(file_path, corruption_offset);

        let (corrupted_segment_id, corrupted_offset_marker) =
            find_corrupted_segment(sr, opts.clone());

        repair(
            &mut clog,
            MAX_TX_ENTRIES,
            corrupted_segment_id,
            corrupted_offset_marker,
        )
        .unwrap();

        // drop lock over commit log
        drop(clog);
    }

    #[allow(unused_assignments)]
    fn find_corrupted_segment(sr: Vec<SegmentRef>, opts: Options) -> (u64, u64) {
        let mut corrupted_segment_id = 0;
        let mut corrupted_offset_marker = 0;
        // Read and repair the corrupted segment
        {
            let reader = Reader::new_from(
                MultiSegmentReader::new(sr).expect("should create"),
                opts.max_segment_size,
                1000,
            );
            let mut tx_reader = TxReader::new(reader);
            let mut tx = TxRecord::new(opts.max_tx_entries as usize);

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
                            corrupted_segment_id = err.segment_id;
                            corrupted_offset_marker = err.offset;
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

        (corrupted_segment_id, corrupted_offset_marker)
    }

    // File header is 170 bytes
    // Each transaction header is 32 in len
    fn corrupt_segment(segment_file_path: &Path, offset_to_edit: u64) {
        // Open the segment file for reading and writing
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment_file_path)
            .expect("should open");

        // Read the file header and move the cursor to the first record
        let header_len = read_file_header(&mut file).expect("should read");

        // Corrupt the checksum of the second record
        let offset_to_edit = header_len.len() as u64 + offset_to_edit;
        let new_byte_value = 0x55;
        file.seek(SeekFrom::Start(offset_to_edit as u64))
            .expect("should seek");
        file.write_all(&[new_byte_value]).expect("should write");
        file.sync_data().unwrap();
    }

    #[tokio::test]
    async fn repair_with_one_record_per_segment() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_segment_size = 55;

        let keys = vec![Bytes::from("k1"), Bytes::from("k2")];
        let default_value = Bytes::from("val");

        let store = setup_store_with_data(opts.clone(), keys, default_value.clone()).await;
        let corruption_offset = 25; // 32bytes is length of txn header
        corrupt_and_repair(&store, opts.clone(), 2, corruption_offset).await;

        // Check if a new transaction can be appended post repair
        let new_keys = vec![Bytes::from("k3")];
        append_data(&store, new_keys, &default_value).await;

        // Close the store
        store.close().await.expect("should close store");
        drop(store);

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        let expected_keys = vec!["k1", "k3"];
        for key in expected_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            let val = txn.get(&key).unwrap().unwrap();
            assert_eq!(val, default_value);
        }
    }

    #[tokio::test]
    async fn repair_with_two_records_per_segment() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_segment_size = 110;

        let keys = vec![
            Bytes::from("k1"),
            Bytes::from("k2"),
            Bytes::from("k3"),
            Bytes::from("k4"),
            Bytes::from("k5"),
        ];
        let default_value = Bytes::from("val");

        let store = setup_store_with_data(opts.clone(), keys, default_value.clone()).await;
        let corruption_offset = 25; // 32bytes is length of txn header
        corrupt_and_repair(&store, opts.clone(), 3, corruption_offset).await;

        // Check if a new transaction can be appended post repair
        let new_keys = vec![Bytes::from("k6")];
        append_data(&store, new_keys, &default_value).await;

        // Close the store
        store.close().await.expect("should close store");
        drop(store);

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        let expected_keys = vec!["k1", "k2", "k3", "k4", "k6"];
        for key in expected_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            let val = txn.get(&key).unwrap().unwrap();
            assert_eq!(val, default_value);
        }
    }


    #[tokio::test]
    async fn repair_with_corruption_in_first_segment() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_segment_size = 55;

        let keys = vec![
            Bytes::from("k1"),
            Bytes::from("k2"),
            Bytes::from("k3"),
            Bytes::from("k4"),
            Bytes::from("k5"),
        ];
        let default_value = Bytes::from("val");

        let store = setup_store_with_data(opts.clone(), keys, default_value.clone()).await;
        let corruption_offset = 25; // 32bytes is length of txn header
        corrupt_and_repair(&store, opts.clone(), 1,corruption_offset).await;

        // Check if a new transaction can be appended post repair
        let new_keys = vec![Bytes::from("k6")];
        append_data(&store, new_keys, &default_value).await;

        // Close the store
        store.close().await.expect("should close store");
        drop(store);

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        let expected_keys = vec!["k6"];
        for key in expected_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            let val = txn.get(&key).unwrap().unwrap();
            assert_eq!(val, default_value);
        }
    }

    #[tokio::test]
    async fn repair_with_corruption_in_middle_segment() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_segment_size = 55;

        let keys = vec![
            Bytes::from("k1"),
            Bytes::from("k2"),
            Bytes::from("k3"),
            Bytes::from("k4"),
            Bytes::from("k5"),
        ];
        let default_value = Bytes::from("val");

        let store = setup_store_with_data(opts.clone(), keys, default_value.clone()).await;
        let corruption_offset = 25; // 32bytes is length of txn header
        corrupt_and_repair(&store, opts.clone(), 4,corruption_offset).await;

        // Check if a new transaction can be appended post repair
        let new_keys = vec![Bytes::from("k6")];
        append_data(&store, new_keys, &default_value).await;

        // Close the store
        store.close().await.expect("should close store");
        drop(store);

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        let expected_keys = vec!["k1", "k2", "k3", "k6"];
        for key in expected_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            let val = txn.get(&key).unwrap().unwrap();
            assert_eq!(val, default_value);
        }
    }

    #[tokio::test]
    async fn repair_single_segment_with_multiple_records() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_segment_size = 550;

        let keys = vec![Bytes::from("k1"), Bytes::from("k2"), Bytes::from("k3"), Bytes::from("k4"), Bytes::from("k5"), Bytes::from("k6")];
        let default_value = Bytes::from("val");

        let store = setup_store_with_data(opts.clone(), keys, default_value.clone()).await;
        let corruption_offset = 220 + 25; // 32bytes is length of txn header
        corrupt_and_repair(&store, opts.clone(), 1, corruption_offset).await;

        // Check if a new transaction can be appended post repair
        let new_keys = vec![Bytes::from("k7")];
        append_data(&store, new_keys, &default_value).await;

        // Close the store
        store.close().await.expect("should close store");
        drop(store);

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        let expected_keys = vec!["k1", "k2", "k3", "k4", "k7"];
        for key in expected_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            let val = txn.get(&key).unwrap().unwrap();
            assert_eq!(val, default_value);
        }
    }

}
