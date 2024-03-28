use std::fs;
use std::path::Path;
use std::path::PathBuf;

use crate::storage::{
    kv::{
        error::{Error, Result},
        option::Options,
        reader::{Reader, TxReader},
        util::sanitize_directory,
    },
    log::{aof::log::Aol, Error as LogError, MultiSegmentReader, Segment, SegmentRef, BLOCK_SIZE},
};

/// The last active segment being written to in the append-only log (AOL) is usually the WAL in database terminology.
/// Corruption in the last segment can happen due to various reasons such as a power failure or a bug in the system,
/// and also due to the asynchronous nature of calling close on the store.
pub(crate) fn repair_last_corrupted_segment(
    aol: &mut Aol,
    db_opts: &Options,
    corrupted_segment_id: u64,
    corrupted_offset_marker: u64,
) -> Result<()> {
    // Read the list of segments from the directory
    let segs = SegmentRef::read_segments_from_directory(&aol.dir)?;

    // Get the last segment
    let last_segment = segs
        .last()
        .ok_or_else(|| Error::LogError(LogError::SegmentNotFound))?;

    // Check if the last segment's ID is equal to the corrupted_segment_id
    if last_segment.id != corrupted_segment_id {
        return Err(Error::MismatchedSegmentID(
            last_segment.id,
            corrupted_segment_id,
        ));
    }

    let last_segment_id = last_segment.id;
    let last_segment_path = last_segment.file_path.clone();
    let last_segment_header_offset = last_segment.file_header_offset;

    drop(segs);

    // Repair the last segment
    repair_segment(
        aol,
        db_opts,
        last_segment_id,
        corrupted_offset_marker,
        last_segment_path,
        last_segment_header_offset,
    )
}

/// This function is used to repair a corrupted any given segment in the append-only log (AOL).
/// Currently it is only being used for testing purposes.
#[allow(unused)]
pub(crate) fn repair_corrupted_segment(
    aol: &mut Aol,
    db_opts: &Options,
    corrupted_segment_id: u64,
    corrupted_offset_marker: u64,
) -> Result<()> {
    // Read the list of segments from the directory
    let segs = SegmentRef::read_segments_from_directory(&aol.dir)?;

    let mut file_path = None;
    let mut file_header_offset = 0;
    // Loop through the segments
    for s in &segs {
        // If the segment ID matches the corrupted segment ID, repair the segment
        if s.id == corrupted_segment_id {
            file_path = Some(s.file_path.clone());
            file_header_offset = s.file_header_offset;
            break;
        }
    }

    // Explicitly drop the segments here to ensure no file descriptors are kept
    drop(segs);

    // Check if file_path is found, if not return an error
    let file_path = match file_path {
        Some(path) => path,
        None => return Ok(()),
    };

    repair_segment(
        aol,
        db_opts,
        corrupted_segment_id,
        corrupted_offset_marker,
        file_path,
        file_header_offset,
    )?;

    Ok(())
}

/// This function repairs a corrupted segment in the append-only log (AOL).
///
/// # Arguments
///
/// * `corrupted_segment_id` - The ID of the corrupted segment to be repaired.
/// * `corrupted_offset_marker` - The offset marker indicating the point up to which records are to be repaired.
///
/// # Returns
///
/// Returns an `Result` indicating the success of the repair operation.
///
/// The function first reads the list of segments from the directory and prepares to store information about the corrupted segment.
/// It then loops through the segments, closing the active segment if its ID matches the corrupted segment's ID and storing information about the corrupted segment.
///
/// If information about the corrupted segment is not available, the function returns an error.
/// Otherwise, it prepares the path for the repaired segment and renames the corrupted segment to the repaired segment.
///
/// The function then opens a new segment as the active segment and creates a segment reader for the repaired segment.
/// It initializes a reader for the segment and reads records until the offset marker is reached, appending the records to the new segment.
///
/// After reading the records, the function flushes and closes the active segment and removes the repaired segment file.
/// If no records were read, it also removes the corrupted segment file.
///
/// Finally, the function opens the next segment and makes it active.
///
/// If any of these operations fail, the function returns an error.
fn repair_segment(
    aol: &mut Aol,
    db_opts: &Options,
    corrupted_segment_id: u64,
    corrupted_offset_marker: u64,
    corrupted_segment_file_path: PathBuf,
    corrupted_segment_file_header_offset: u64,
) -> Result<()> {
    // Close the active segment if its ID matches
    if aol.active_segment_id == corrupted_segment_id {
        aol.active_segment.close()?;
    }

    // Prepare the repaired segment path
    let repaired_segment_path = corrupted_segment_file_path.with_extension("repair");

    // Rename the corrupted segment to the repaired segment
    std::fs::rename(&corrupted_segment_file_path, &repaired_segment_path)?;

    // Open a new segment as the active segment
    let mut new_segment: Segment<0> = Segment::open(&aol.dir, corrupted_segment_id, &aol.opts)?;

    // Create a segment reader for the repaired segment
    let segments: Vec<SegmentRef> = vec![SegmentRef {
        file_path: repaired_segment_path.clone(),
        file_header_offset: corrupted_segment_file_header_offset,
        id: corrupted_segment_id,
    }];
    let segment_reader = MultiSegmentReader::new(segments)?;

    // Initialize a reader for the segment
    let reader = Reader::new_from(segment_reader, aol.opts.max_file_size, BLOCK_SIZE);
    let mut reader = TxReader::new(reader, db_opts.max_key_size, db_opts.max_value_size);

    let mut count = 0;
    // Read records until the offset marker is reached
    while let Ok((data, cur_offset)) = reader.read(db_opts.max_entries_per_txn as usize) {
        if cur_offset >= corrupted_offset_marker {
            break;
        }

        new_segment.append(data)?;
        count += 1;
    }

    // Flush and close the active segment
    new_segment.close()?;

    // Remove the repaired segment file
    std::fs::remove_file(&repaired_segment_path)?;

    // Open the next segment and make it active
    if count == 0 {
        println!("deleting empty file {:?}", corrupted_segment_file_path);
        std::fs::remove_file(&corrupted_segment_file_path)?;
    }
    let new_segment = Segment::open(&aol.dir, aol.active_segment_id, &aol.opts)?;
    aol.active_segment = new_segment;

    Ok(())
}

// This function is used to restore files in a directory after a repair process.
// The repair process creates a backup of the original file with a '.repair' extension
// and creates a new file with the '.clog' extension. If the repair process is interrupted,
// there might be pairs of '.clog' and '.repair' files in the directory.
// This function deletes the '.clog' file and renames the '.repair' file back to '.clog'.
//
// Parameters:
// directory: A string slice that holds the path to the directory.
pub(crate) fn restore_repair_files(directory: &str) -> std::io::Result<()> {
    // Check if the directory exists
    if !Path::new(directory).exists() {
        return Ok(());
    }

    // Validate and sanitize the directory parameter
    let directory = sanitize_directory(directory)?;

    // Read the directory
    let entries = fs::read_dir(directory.clone())?;

    // Iterate over each entry in the directory
    for entry in entries.flatten() {
        // If the entry is a file
        let path = entry.path();
        if path.is_file() {
            // Get the filename of the file
            let filename = path.file_name().unwrap().to_str().unwrap();
            // If the filename ends with '.repair'
            if let Some(stem) = filename.strip_suffix(".repair") {
                // Construct the filename of the corresponding '.clog' file
                let clog_filename = format!("{}.clog", stem);
                let clog_path = Path::new(&directory).join(clog_filename);
                // If the '.clog' file exists
                if clog_path.exists() {
                    // Remove the '.clog' file
                    fs::remove_file(&clog_path)?;
                }
                // Rename the '.repair' file back to '.clog'
                fs::rename(path, clog_path)?;
            }
        }
    }

    // Return Ok if the operation was successful
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::fs::OpenOptions;
    use std::io::Read;
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

    use std::process::Command;
    use std::str;

    #[allow(unused)]
    pub fn count_file_descriptors(file_path: &str) -> std::io::Result<usize> {
        let output = Command::new("lsof").arg(file_path).output()?;

        if output.status.success() {
            let output_str = str::from_utf8(&output.stdout).unwrap();
            let count = output_str.lines().count();
            // Subtract 1 for the header line
            Ok(if count > 0 { count - 1 } else { 0 })
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to execute lsof",
            ))
        }
    }

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

    fn corrupt_at_offset(opts: Options, segment_num: usize, corruption_offset: u64) {
        let clog_subdir = opts.dir.join("clog");
        let sr =
            SegmentRef::read_segments_from_directory(&clog_subdir).expect("should read segments");

        // Open the nth segment file for corrupting
        let file_path = &sr[segment_num - 1].file_path;
        corrupt_segment(file_path, corruption_offset);
    }

    // Helper function to corrupt any segment file at a given offset
    // This is useful for testing the repair process because when opening
    // the store, the repair process will only repair the corrupted segment
    // if it is the last segment in the directory.
    fn corrupt_and_repair(
        store: &Store,
        opts: Options,
        segment_num: usize,
        corruption_offset: u64,
    ) {
        let mut clog = store.inner.as_ref().unwrap().core.clog.write();

        let clog_subdir = opts.dir.join("clog");
        let sr =
            SegmentRef::read_segments_from_directory(&clog_subdir).expect("should read segments");

        // Open the nth segment file for corruption
        corrupt_at_offset(opts.clone(), segment_num, corruption_offset);

        let (corrupted_segment_id, corrupted_offset_marker) =
            find_corrupted_segment(sr, opts.clone());

        repair_corrupted_segment(
            &mut clog,
            &opts,
            corrupted_segment_id,
            corrupted_offset_marker,
        )
        .unwrap();

        // drop lock over commit log
        drop(clog);
    }

    #[allow(unused)]
    fn find_corrupted_segment(sr: Vec<SegmentRef>, opts: Options) -> (u64, u64) {
        let reader = Reader::new_from(
            MultiSegmentReader::new(sr).expect("should create"),
            opts.max_segment_size,
            1000,
        );
        let mut tx_reader = TxReader::new(reader, opts.max_key_size, opts.max_value_size);
        let mut tx = TxRecord::new(opts.max_entries_per_txn as usize);

        loop {
            tx.reset();

            match tx_reader.read_into(&mut tx) {
                Ok(_) => continue,
                Err(Error::LogError(LogError::Corruption(err))) => {
                    return (err.segment_id, err.offset);
                }
                Err(err) => panic!(
                    "Expected a CorruptionError, but got a different error {}",
                    err
                ),
            }
        }
    }

    // File header is 170 bytes
    // Each transaction header is 24 in len
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
        let new_byte_value = 0x47;
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
        opts.max_segment_size = 47;

        let keys = vec![Bytes::from("k1"), Bytes::from("k2")];
        let default_value = Bytes::from("val");

        let store = setup_store_with_data(opts.clone(), keys, default_value.clone()).await;
        let corruption_offset = 17; // 24bytes is length of txn header
        corrupt_and_repair(&store, opts.clone(), 2, corruption_offset);

        // Close the store
        store.close().await.expect("should close store");

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        // Check if a new transaction can be appended post repair
        let new_keys = vec![Bytes::from("k3")];
        append_data(&store, new_keys, &default_value).await;

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
        let corruption_offset = 17; // 24bytes is length of txn header
        corrupt_and_repair(&store, opts.clone(), 3, corruption_offset);

        // Close the store
        store.close().await.expect("should close store");

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        // Entries after corruption offset should not be present
        let expected_deleted_keys = vec!["k5"];
        for key in expected_deleted_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            assert_eq!(txn.get(&key).unwrap(), None);
        }

        // Check if a new transaction can be appended post repair
        let new_keys = vec![Bytes::from("k7")];
        append_data(&store, new_keys, &default_value).await;

        let expected_keys = vec!["k1", "k2", "k3", "k4", "k7"];
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
        opts.max_segment_size = 47;

        let keys = vec![
            Bytes::from("k1"),
            Bytes::from("k2"),
            Bytes::from("k3"),
            Bytes::from("k4"),
            Bytes::from("k5"),
        ];
        let default_value = Bytes::from("val");

        let store = setup_store_with_data(opts.clone(), keys, default_value.clone()).await;
        let corruption_offset = 17; // 24bytes is length of txn header
        corrupt_and_repair(&store, opts.clone(), 1, corruption_offset);

        // Close the store
        store.close().await.expect("should close store");

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        // Entries after corruption offset should not be present
        let expected_deleted_keys = vec!["k1"];
        for key in expected_deleted_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            assert_eq!(txn.get(&key).unwrap(), None);
        }

        let expected_keys = vec!["k2", "k3", "k4", "k5"];
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
        opts.max_segment_size = 47;

        let keys = vec![
            Bytes::from("k1"),
            Bytes::from("k2"),
            Bytes::from("k3"),
            Bytes::from("k4"),
            Bytes::from("k5"),
        ];
        let default_value = Bytes::from("val");

        let store = setup_store_with_data(opts.clone(), keys, default_value.clone()).await;
        let corruption_offset = 17; // 24bytes is length of txn header
        corrupt_and_repair(&store, opts.clone(), 3, corruption_offset);

        // Close the store
        store.close().await.expect("should close store");

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        // Entries after corruption offset should not be present
        let expected_deleted_keys = vec!["k3"];
        for key in expected_deleted_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            assert_eq!(txn.get(&key).unwrap(), None);
        }

        let expected_keys = vec!["k1", "k2", "k4", "k5"];
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
        opts.max_segment_size = 470;

        let keys = vec![
            Bytes::from("k1"),
            Bytes::from("k2"),
            Bytes::from("k3"),
            Bytes::from("k4"),
            Bytes::from("k5"),
            Bytes::from("k6"),
        ];
        let default_value = Bytes::from("val");

        let store = setup_store_with_data(opts.clone(), keys, default_value.clone()).await;
        let corruption_offset = 220 + 17; // 24bytes is length of txn header
        corrupt_at_offset(opts.clone(), 1, corruption_offset);

        // Check if a new transaction can be appended post repair
        let new_keys = vec![Bytes::from("k7")];
        append_data(&store, new_keys, &default_value).await;

        // Close the store
        store.close().await.expect("should close store");

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        // Entries after corruption offset should not be present
        let expected_deleted_keys = vec!["k5", "k6", "k7"];
        for key in expected_deleted_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            assert_eq!(txn.get(&key).unwrap(), None);
        }

        // Check if a new transaction can be appended post repair
        let new_keys = vec![Bytes::from("k8")];
        append_data(&store, new_keys, &default_value).await;

        let expected_keys = vec!["k1", "k2", "k3", "k4", "k8"];
        for key in expected_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            let val = txn.get(&key).unwrap().unwrap();
            assert_eq!(val, default_value);
        }
    }

    #[tokio::test]
    async fn repair_multiple_segment_with_multiple_records_with_corruption_in_first_segment() {
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
            Bytes::from("k6"),
        ];
        let default_value = Bytes::from("val");

        let store = setup_store_with_data(opts.clone(), keys, default_value.clone()).await;
        let corruption_offset = 47 + 17; // 24bytes is length of txn header
        corrupt_and_repair(&store, opts.clone(), 1, corruption_offset);

        // Close the store
        store.close().await.expect("should close store");

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        // Check if a new transaction can be appended post repair
        let new_keys = vec![Bytes::from("k7")];
        append_data(&store, new_keys, &default_value).await;

        let expected_keys = vec!["k1", "k3", "k4", "k5", "k6", "k7"];
        for key in expected_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            let val = txn.get(&key).unwrap().unwrap();
            assert_eq!(val, default_value);
        }
    }

    #[tokio::test]
    async fn repair_multiple_segment_with_multiple_records_with_corruption_in_middle_segment() {
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
            Bytes::from("k6"),
        ];
        let default_value = Bytes::from("val");

        let store = setup_store_with_data(opts.clone(), keys, default_value.clone()).await;
        let corruption_offset = 47 + 17; // 24bytes is length of txn header
        corrupt_and_repair(&store, opts.clone(), 2, corruption_offset);

        // Close the store
        store.close().await.expect("should close store");

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        // Check if a new transaction can be appended post repair
        let new_keys = vec![Bytes::from("k7")];
        append_data(&store, new_keys, &default_value).await;

        let expected_keys = vec!["k1", "k2", "k3", "k5", "k6", "k7"];
        for key in expected_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            let val = txn.get(&key).unwrap().unwrap();
            assert_eq!(val, default_value);
        }
    }

    #[tokio::test]
    async fn repair_multiple_segment_with_multiple_records_with_corruption_in_last_segment() {
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
            Bytes::from("k6"),
        ];
        let default_value = Bytes::from("val");

        let store = setup_store_with_data(opts.clone(), keys, default_value.clone()).await;
        let corruption_offset = 47 + 17; // 24bytes is length of txn header
        corrupt_at_offset(opts.clone(), 3, corruption_offset);

        // Close the store
        store.close().await.expect("should close store");

        // Restart store to see if all entries are read
        let store = Store::new(opts.clone()).expect("should create store");

        // Entries after corruption offset should not be present
        let expected_deleted_keys = vec!["k6"];
        for key in expected_deleted_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            assert_eq!(txn.get(&key).unwrap(), None);
        }

        // Check if a new transaction can be appended post repair
        let new_keys = vec![Bytes::from("k7")];
        append_data(&store, new_keys, &default_value).await;

        let expected_keys = vec!["k1", "k2", "k3", "k4", "k5", "k7"];
        for key in expected_keys {
            let key = Bytes::from(key);

            // Start a new read-write transaction (txn)
            let txn = store.begin().unwrap();
            let val = txn.get(&key).unwrap().unwrap();
            assert_eq!(val, default_value);
        }
    }

    #[test]
    fn test_restore_files_multiple_clog_files() {
        let dir = create_temp_directory();
        let mut file1 = File::create(dir.path().join("0001.clog")).unwrap();
        let mut file2 = File::create(dir.path().join("0002.clog")).unwrap();
        file1.write_all(b"Data for 0001.clog").unwrap();
        file2.write_all(b"Data for 0002.clog").unwrap();
        assert!(restore_repair_files(dir.path().to_str().unwrap()).is_ok());
        assert!(dir.path().join("0001.clog").exists());
        assert!(dir.path().join("0002.clog").exists());
    }

    #[test]
    fn test_restore_files_multiple_repair_files() {
        let dir = create_temp_directory();
        File::create(dir.path().join("001.repair")).unwrap();
        File::create(dir.path().join("002.repair")).unwrap();
        assert!(restore_repair_files(dir.path().to_str().unwrap()).is_ok());
        assert!(dir.path().join("001.clog").exists());
        assert!(dir.path().join("002.clog").exists());
        assert!(!dir.path().join("001.repair").exists());
        assert!(!dir.path().join("002.repair").exists());
    }

    #[test]
    fn test_restore_files_multiple_clog_and_repair_files() {
        let dir = create_temp_directory();
        File::create(dir.path().join("0001.clog")).unwrap();
        File::create(dir.path().join("0002.clog")).unwrap();
        File::create(dir.path().join("0001.repair")).unwrap();
        File::create(dir.path().join("0002.repair")).unwrap();
        assert!(restore_repair_files(dir.path().to_str().unwrap()).is_ok());
        assert!(dir.path().join("0001.clog").exists());
        assert!(dir.path().join("0002.clog").exists());
        assert!(!dir.path().join("0001.repair").exists());
        assert!(!dir.path().join("0002.repair").exists());
    }

    #[test]
    fn test_restore_files_single_clog_and_repair_files() {
        let dir = create_temp_directory();
        File::create(dir.path().join("0001.clog")).unwrap();
        File::create(dir.path().join("0002.clog")).unwrap();
        File::create(dir.path().join("0001.repair")).unwrap();
        assert!(restore_repair_files(dir.path().to_str().unwrap()).is_ok());
        assert!(dir.path().join("0001.clog").exists());
        assert!(dir.path().join("0002.clog").exists());
        assert!(!dir.path().join("0001.repair").exists());
    }

    #[test]
    fn test_restore_files_multiple_repair_files_with_data() {
        let dir = create_temp_directory();
        let mut file1 = File::create(dir.path().join("0001.clog")).unwrap();
        let mut file2 = File::create(dir.path().join("0002.clog")).unwrap();
        let mut repair_file1 = File::create(dir.path().join("0001.repair")).unwrap();
        let mut repair_file2 = File::create(dir.path().join("0002.repair")).unwrap();
        file1.write_all(b"Data for 0001.clog").unwrap();
        file2.write_all(b"Data for 0002.clog").unwrap();
        repair_file1.write_all(b"Data for 0001.repair").unwrap();
        repair_file2.write_all(b"Data for 0002.repair").unwrap();
        assert!(restore_repair_files(dir.path().to_str().unwrap()).is_ok());
        assert!(dir.path().join("0001.clog").exists());
        assert!(dir.path().join("0002.clog").exists());
        assert!(!dir.path().join("0001.repair").exists());
        assert!(!dir.path().join("0002.repair").exists());

        let mut restored_file1 = File::open(dir.path().join("0001.clog")).unwrap();
        let mut restored_file2 = File::open(dir.path().join("0002.clog")).unwrap();
        let mut contents1 = String::new();
        let mut contents2 = String::new();
        restored_file1.read_to_string(&mut contents1).unwrap();
        restored_file2.read_to_string(&mut contents2).unwrap();
        assert_eq!(contents1, "Data for 0001.repair");
        assert_eq!(contents2, "Data for 0002.repair");
    }
}
