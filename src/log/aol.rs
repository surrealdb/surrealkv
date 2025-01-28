use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;
use quick_cache::sync::Cache;

use crate::log::{get_segment_range, Error, IOError, Options, Result, Segment};

/// Writer state that needs exclusive access
pub(crate) struct WriterState {
    /// The currently active segment where data is being written.
    pub(crate) active_segment: Segment,
    /// The ID of the currently active segment.
    pub(crate) active_segment_id: u64,
}

/// Append-Only Log (Aol) is a data structure used to sequentially store records
/// in a series of segments. It provides efficient write operations,
/// making it suitable for use cases like storing large amounts of data and
/// writing data in a sequential manner.
pub struct Aol {
    /// The write state protected by a mutex
    pub(crate) writer_state: Mutex<WriterState>,

    /// The directory where the segment files are located.
    pub(crate) dir: PathBuf,

    /// Configuration options for the AOL instance.
    pub(crate) opts: Options,

    /// A flag indicating whether the AOL instance is closed or not.
    closed: AtomicBool,

    /// Lock-free cache for all read segments (including active)
    segment_cache: Cache<u64, Arc<Segment>>,

    /// A flag indicating whether the AOL instance has encountered an IO error or not.
    fsync_failed: AtomicBool,
}

impl Aol {
    /// Opens or creates a new AOL instance associated with the specified directory and segment ID.
    ///
    /// This function prepares the AOL instance by creating the necessary directory,
    /// determining the active segment ID, and initializing the active segment.
    pub fn open(dir: &Path, opts: &Options) -> Result<Self> {
        // Ensure the options are valid
        opts.validate()?;

        // Ensure the directory exists with proper permissions
        Self::prepare_directory(dir, opts)?;

        // Determine the active segment ID
        let active_segment_id = Self::calculate_current_write_segment_id(dir)?;

        // Open the active segment
        let active_segment = Segment::open(dir, active_segment_id, opts, false)?;

        let writer_state = WriterState {
            active_segment,
            active_segment_id,
        };

        Ok(Self {
            writer_state: Mutex::new(writer_state),
            dir: dir.to_path_buf(),
            opts: opts.clone(),
            closed: AtomicBool::new(false),
            segment_cache: Cache::new(opts.max_cached_segments),
            fsync_failed: Default::default(),
        })
    }

    // Helper function to prepare the directory with proper permissions
    fn prepare_directory(dir: &Path, opts: &Options) -> Result<()> {
        fs::create_dir_all(dir)?;

        if let Ok(metadata) = fs::metadata(dir) {
            let mut permissions = metadata.permissions();

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                permissions.set_mode(opts.dir_mode.unwrap_or(0o750));
            }

            #[cfg(windows)]
            {
                permissions.set_readonly(false);
            }

            fs::set_permissions(dir, permissions)?;
        }

        Ok(())
    }

    // Helper function to calculate the active segment ID
    fn calculate_current_write_segment_id(dir: &Path) -> Result<u64> {
        let (_, last) = get_segment_range(dir)?;
        Ok(last)
    }

    /// Appends a record to the active segment.
    pub fn append(&self, rec: &[u8]) -> Result<(u64, u64, usize)> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::SegmentClosed);
        }

        self.check_if_fsync_failed()?;

        if rec.is_empty() {
            return Err(Error::EmptyBuffer);
        }

        // Check if the record is larger than the maximum file size
        if rec.len() > self.opts.max_file_size as usize {
            return Err(Error::RecordTooLarge);
        }

        // Take write lock only when needed
        let mut writer = self.writer_state.lock();

        // Check if we need to rotate
        let current_offset = writer.active_segment.offset();
        // Calculate available space in the active segment
        let available = self.opts.max_file_size as i64 - current_offset as i64;

        // If the entire record can't fit into the remaining space of the current segment,
        // close the current segment and create a new one
        if available < rec.len() as i64 {
            // Rotate to new segment
            // Sync and close the active segment
            // Note that closing the segment will
            // not close the underlying file until
            // it is dropped.
            writer.active_segment.close()?;

            // Increment active segment id and get the new id
            writer.active_segment_id += 1;

            // Open new segment with the incremented id
            let new_segment =
                Segment::open(&self.dir, writer.active_segment_id, &self.opts, false)?;

            writer.active_segment = new_segment;
        }

        // Write the record to the segment
        match writer.active_segment.append(rec) {
            Ok(offset) => Ok((writer.active_segment_id, offset, rec.len())),
            Err(e) => {
                if let Error::IO(_) = e {
                    self.set_fsync_failed(true);
                }
                Err(e)
            }
        }
    }

    /// Flushes and syncs the active segment.
    pub fn sync(&self) -> Result<()> {
        self.check_if_fsync_failed()?;
        let writer = self.writer_state.lock();
        writer.active_segment.sync()
    }

    /// Reads data from the segment at the specified offset into the provided buffer.
    pub fn read_at(
        &self,
        buf: &mut [u8],
        segment_id: u64,
        read_offset: u64,
    ) -> Result<(u64, usize)> {
        self.check_if_fsync_failed()?;

        if buf.is_empty() {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::UnexpectedEof,
                "Buffer is empty",
            )));
        }

        // Get segment from cache (works for both active and inactive segments)
        let segment = self.segment_cache.get_or_insert_with(&segment_id, || {
            Segment::open(&self.dir, segment_id, &self.opts, true)
                .map(Arc::new)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;

        match segment.read_at(buf, read_offset) {
            Ok(bytes_read) => Ok((segment_id, bytes_read)),
            Err(Error::Eof) => Err(Error::Eof),
            Err(e) => Err(e),
        }
    }

    pub fn close(&self) -> Result<()> {
        let writer = self.writer_state.lock();
        writer.active_segment.close()?;
        // Clear segment cache to ensure all file descriptors are dropped
        self.segment_cache.clear();
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    pub fn rotate(&self) -> Result<u64> {
        let mut writer = self.writer_state.lock();

        // Close current segment
        writer.active_segment.close()?;

        // Increment active segment id and get the new id
        writer.active_segment_id += 1;

        // Open new segment with the incremented id
        writer.active_segment =
            Segment::open(&self.dir, writer.active_segment_id, &self.opts, false)?;

        Ok(writer.active_segment_id)
    }

    pub fn size(&self) -> Result<u64> {
        let writer = self.writer_state.lock();
        let cur_segment_size = writer.active_segment.file_offset();
        let total_size = (writer.active_segment_id * self.opts.max_file_size) + cur_segment_size;
        Ok(total_size)
    }

    #[inline]
    fn set_fsync_failed(&self, failed: bool) {
        self.fsync_failed.store(failed, Ordering::Release);
    }

    /// Checks if fsync failed and returns error if true. Otherwise, returns Ok.
    /// This means writes or reads on the log file will not happpen if fsync failed previously.
    /// The only way to recorver is to close and restart the store to repair the corruped log file.
    #[inline]
    fn check_if_fsync_failed(&self) -> Result<()> {
        if self.fsync_failed.load(Ordering::Acquire) {
            Err(Error::IO(IOError::new(
                io::ErrorKind::Other,
                "fsync failed",
            )))
        } else {
            Ok(())
        }
    }
}

impl Drop for Aol {
    /// Attempt to fsync data on drop, in case we're running without sync.
    fn drop(&mut self) {
        self.close().ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    fn get_writer_state(aol: &Aol) -> (u64, u64) {
        let writer = aol.writer_state.lock();
        (writer.active_segment_id, writer.active_segment.offset())
    }

    #[test]
    fn append() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options::default();
        let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Test initial offset
        let sz = get_writer_state(&a);
        assert_eq!(0, sz.0);
        assert_eq!(0, sz.1);

        // Test appending an empty buffer
        let r = a.append(&[]);
        assert!(r.is_err());

        // Test appending a non-empty buffer
        let r = a.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().2);

        // Test appending another buffer
        let r = a.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());
        assert_eq!(7, r.unwrap().2);

        let (segment_id, offset) = get_writer_state(&a);

        // Validate offset after appending
        // 4 + 7 = 11
        assert_eq!(offset, 11);

        // Test reading from segment
        let mut bs = vec![0; 4];
        let n = a.read_at(&mut bs, segment_id, 0).expect("should read");
        assert_eq!(4, n.1);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[..]);

        // Test reading another portion of data from segment
        let mut bs = vec![0; 7];
        let n = a.read_at(&mut bs, segment_id, 4).expect("should read");
        assert_eq!(7, n.1);
        assert_eq!(&[4, 5, 6, 7, 8, 9, 10].to_vec(), &bs[..]);

        // Test reading beyond segment's current size
        let mut bs = vec![0; 15];
        let r = a.read_at(&mut bs, segment_id, 4097).unwrap();
        assert_eq!(r, (0, 0));
        assert_eq!(bs, vec![0; 15]);

        // Test appending another buffer
        let r = a.append(&[11, 12, 13, 14]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().2);

        let (segment_id, offset) = get_writer_state(&a);
        // Validate offset after appending
        // 11 + 4 = 15
        assert_eq!(offset, 15);

        // Test reading from segment after appending
        let mut bs = vec![0; 4];
        let n = a.read_at(&mut bs, segment_id, 11).expect("should read");
        assert_eq!(4, n.1);
        assert_eq!(&[11, 12, 13, 14].to_vec(), &bs[..]);

        // Test closing segment
        assert!(a.close().is_ok());
    }

    #[test]
    fn append_and_read_two_blocks() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options::default();
        let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Create two slices of bytes of different sizes
        let data1 = vec![1; 31 * 1024];
        let data2 = vec![2; 2 * 1024];

        // Append the first data slice to the aol
        let r1 = a.append(&data1);
        assert!(r1.is_ok());
        assert_eq!(31 * 1024, r1.unwrap().2);

        // Append the second data slice to the aol
        let r2 = a.append(&data2);
        assert!(r2.is_ok());
        assert_eq!(2 * 1024, r2.unwrap().2);

        let (segment_id, _) = get_writer_state(&a);

        // Read the first data slice back from the aol
        let mut read_data1 = vec![0; 31 * 1024];
        let n1 = a
            .read_at(&mut read_data1, segment_id, 0)
            .expect("should read");
        assert_eq!(31 * 1024, n1.1);
        assert_eq!(data1, read_data1);

        // Read the second data slice back from the aol
        let mut read_data2 = vec![0; 2 * 1024];
        let n2 = a
            .read_at(&mut read_data2, segment_id, 31 * 1024)
            .expect("should read");
        assert_eq!(2 * 1024, n2.1);
        assert_eq!(data2, read_data2);

        // Test closing segment
        assert!(a.close().is_ok());
    }

    #[test]
    fn append_read_append_read() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options::default();
        let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Create two slices of bytes of different sizes
        let data1 = vec![1; 31 * 1024];
        let data2 = vec![2; 2 * 1024];
        let data3 = vec![3; 1024];
        let data4 = vec![4; 1024];

        // Append the first data slice to the aol
        let r1 = a.append(&data1);
        assert!(r1.is_ok());
        assert_eq!(31 * 1024, r1.unwrap().2);

        // Append the second data slice to the aol
        let r2 = a.append(&data2);
        assert!(r2.is_ok());
        assert_eq!(2 * 1024, r2.unwrap().2);

        // Read the first data slice back from the aol
        let mut read_data1 = vec![0; 31 * 1024];
        let (segment_id, _) = get_writer_state(&a);
        let n1 = a
            .read_at(&mut read_data1, segment_id, 0)
            .expect("should read");
        assert_eq!(31 * 1024, n1.1);
        assert_eq!(data1, read_data1);

        // Append the third data slice to the aol
        let r3 = a.append(&data4);
        assert!(r3.is_ok());
        assert_eq!(1024, r3.unwrap().2);

        // Append the third data slice to the aol
        let r4 = a.append(&data3);
        assert!(r4.is_ok());
        assert_eq!(1024, r4.unwrap().2);

        let (segment_id, _) = get_writer_state(&a);

        // Read the first data slice back from the aol
        let mut read_data1 = vec![0; 31 * 1024];
        let n1 = a
            .read_at(&mut read_data1, segment_id, 0)
            .expect("should read");
        assert_eq!(31 * 1024, n1.1);
        assert_eq!(data1, read_data1);

        // Read the second data slice back from the aol
        let mut read_data2 = vec![0; 2 * 1024];
        let n2 = a
            .read_at(&mut read_data2, segment_id, 31 * 1024)
            .expect("should read");
        assert_eq!(2 * 1024, n2.1);
        assert_eq!(data2, read_data2);

        // Test closing segment
        assert!(a.close().is_ok());
    }

    #[test]
    fn append_large_record() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options {
            max_file_size: 1024,
            ..Default::default()
        };
        let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        let large_record = vec![1; 1025];
        let small_record = vec![1; 1024];
        let r = a.append(&small_record);
        assert!(r.is_ok());
        let (segment_id, offset) = get_writer_state(&a);
        assert_eq!(0, segment_id);
        assert_eq!(1024, offset);

        let r = a.append(&large_record);
        assert!(r.is_err());
        let (segment_id, offset) = get_writer_state(&a);
        assert_eq!(0, segment_id);
        assert_eq!(1024, offset);
    }

    #[test]
    fn fsync_failure() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options {
            max_file_size: 1024,
            ..Default::default()
        };
        let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        let small_record = vec![1; 1024];
        let r = a.append(&small_record);
        assert!(r.is_ok());
        let (segment_id, offset) = get_writer_state(&a);
        assert_eq!(1024, offset);

        // Simulate fsync failure
        a.set_fsync_failed(true);

        // Writes should fail aftet fsync failure
        let r = a.append(&small_record);
        assert!(r.is_err());

        // Reads should fail after fsync failure
        let mut read_data = vec![0; 1024];
        let r = a.read_at(&mut read_data, segment_id, 0);
        assert!(r.is_err());
    }

    #[test]
    fn append_and_reload_to_check_active_segment_id() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options {
            max_file_size: 1024,
            ..Default::default()
        };
        let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        let large_record = vec![1; 1024];
        let small_record = vec![1; 512];
        let r = a.append(&large_record);
        assert!(r.is_ok());
        let (segment_id, offset) = get_writer_state(&a);
        assert_eq!(0, segment_id);
        assert_eq!(1024, offset);

        assert_eq!(0, get_writer_state(&a).0);

        a.close().expect("should close");

        let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");
        assert_eq!(0, get_writer_state(&a).0);

        let r = a.append(&small_record);
        assert!(r.is_ok());
        let (segment_id, offset) = get_writer_state(&a);
        assert_eq!(1, segment_id);
        assert_eq!(512, offset);
        assert_eq!(1, get_writer_state(&a).0);
    }

    #[test]
    fn append_records_across_two_files_and_read() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create AOL options with a small max file size to force a new file creation on overflow
        let opts = Options {
            max_file_size: 1024, // Small enough to ensure the second append creates a new file
            ..Default::default()
        };
        let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Append a record that fits within the first file
        let first_record = vec![1; 512];
        a.append(&first_record)
            .expect("first append should succeed");
        let (first_segment_id, first_offset) = get_writer_state(&a);
        assert_eq!(0, first_segment_id);
        assert_eq!(512, first_offset);

        // Append another record that causes a new file (segment) to be created
        let second_record = vec![2; 1024]; // This will exceed the max_file_size
        a.append(&second_record)
            .expect("second append should succeed");
        let (second_segment_id, second_offset) = get_writer_state(&a);
        assert_eq!(1, second_segment_id); // Expecting a new segment/file
        assert_eq!(1024, second_offset);

        // Read back the first record using its segment ID and offset
        let mut read_buf = vec![0; 512];
        a.read_at(&mut read_buf, first_segment_id, 0) // Start at offset 0 of the first segment
            .expect("failed to read first record");
        assert_eq!(first_record, read_buf, "First record data mismatch");

        // Read back the second record using its segment ID and offset
        let mut read_buf = vec![0; 1024];
        a.read_at(&mut read_buf, second_segment_id, 0) // Start at offset 0 of the second segment
            .expect("failed to read second record");
        assert_eq!(second_record, read_buf, "Second record data mismatch");
    }

    #[test]
    fn read_beyond_current_offset_should_not_fail() {
        // Setup: Create a temporary directory and initialize the log with default options
        let temp_dir = create_temp_directory();
        let opts = Options::default();
        let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Append a single record to ensure there is some data in the log
        let record = vec![1; 512];
        a.append(&record).expect("append should succeed");

        // Attempt to read beyond the current offset
        let mut read_buf = vec![0; 1024]; // Buffer size is larger than the appended record
        let result = a.read_at(&mut read_buf, 0, 1024); // Attempt to read at an offset beyond the single appended record

        // Verify: The read operation should fail or return an error indicating the offset is out of bounds
        assert_eq!(result.unwrap(), (0, 0));
        assert_eq!(read_buf, vec![0; 1024]);
    }

    #[test]
    fn append_after_reopening_log() {
        // Setup: Create a temporary directory and initialize the log
        let temp_dir = create_temp_directory();
        let opts = Options::default();
        let record = vec![1; 512];

        // Step 1: Open the log, append a record, and then close it
        {
            let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");
            a.append(&record).expect("append should succeed");
        } // Log is closed here as `a` goes out of scope

        // Step 2: Reopen the log and append another record
        {
            let a = Aol::open(temp_dir.path(), &opts).expect("should reopen aol");
            a.append(&record)
                .expect("append after reopen should succeed");

            // Verify: Ensure the log contains both records by reading them back
            let mut read_buf = vec![0; 512];
            a.read_at(&mut read_buf, 0, 0)
                .expect("failed to read first record after reopen");
            assert_eq!(record, read_buf, "First record data mismatch after reopen");

            a.read_at(&mut read_buf, 0, 512)
                .expect("failed to read second record after reopen");
            assert_eq!(record, read_buf, "Second record data mismatch after reopen");
        }
    }

    #[test]
    fn append_across_two_files_and_read() {
        // Setup: Create a temporary directory and initialize the log with small max file size
        let temp_dir = create_temp_directory();
        let opts = Options {
            max_file_size: 512, // Set max file size to 512 bytes to force new file creation on second append
            ..Default::default()
        };
        let record = vec![1; 512]; // Each record is 512 bytes

        // Step 1: Open the log, append a record, and then close it
        {
            let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");
            a.append(&record).expect("first append should succeed");
            a.append(&record).expect("first append should succeed");
        } // Log is closed here as `a` goes out of scope

        // Step 2: Reopen the log and append another record, which should create a new file
        {
            let a = Aol::open(temp_dir.path(), &opts).expect("should reopen aol");

            // Verify: Ensure the first record is in a new file by reading it back
            let mut read_buf = vec![0; 512];
            // The first record should be at the start of the first file (segment), hence segment_id = 1 and offset = 0.
            a.read_at(&mut read_buf, 0, 0)
                .expect("failed to read first record from new file");
            assert_eq!(record, read_buf, "Record data mismatch in first file");

            // Verify: Ensure the second record is in a new file by reading it back
            let mut read_buf = vec![0; 512];
            // The second record should be at the start of the second file (segment), hence segment_id = 1 and offset = 0.
            a.read_at(&mut read_buf, 1, 0)
                .expect("failed to read second record from new file");
            assert_eq!(record, read_buf, "Record data mismatch in second file");

            a.append(&record).expect("first append should succeed");
            // Verify: Ensure the third record is in a new file by reading it back

            let mut read_buf = vec![0; 512];
            // The third record should be at the start of the third file (segment), hence segment_id = 1 and offset = 0.
            a.read_at(&mut read_buf, 2, 0)
                .expect("failed to read third record from new file");
            assert_eq!(record, read_buf, "Record data mismatch in third file");
        }
    }

    // TODO: add to benchmarks
    #[test]
    fn sequential_read_performance() {
        let temp_dir = create_temp_directory();
        let opts = Options::default();
        let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Append 1000 records to ensure we have enough data
        let record = vec![1; 512]; // Each record is 512 bytes
        for _ in 0..1000 {
            a.append(&record).expect("append should succeed");
        }

        let start_time = std::time::Instant::now();

        // Sequentially read all records
        let mut read_buf = vec![0; 512];
        for offset in (0..512000).step_by(512) {
            a.read_at(&mut read_buf, 0, offset as u64)
                .expect("failed to read record");
        }

        let duration = start_time.elapsed();

        println!("Sequential read of 1000 records took: {:?}", duration);
    }

    // TODO: add to benchmarks
    #[test]
    fn random_access_read_performance() {
        let temp_dir = create_temp_directory();
        let opts = Options::default();
        let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Append 1000 records to ensure we have enough data
        let record = vec![1; 512]; // Each record is 512 bytes
        for _ in 0..1000 {
            a.append(&record).expect("append should succeed");
        }

        // Generate a list of random offsets within the range of the data written
        let mut offsets = (0..1000)
            .map(|_| rand::thread_rng().gen_range(0..1000) * 512)
            .collect::<Vec<u64>>();
        offsets.sort_unstable();
        offsets.dedup();

        let start_time = std::time::Instant::now();

        // Randomly read records based on the generated offsets
        let mut read_buf = vec![0; 512];
        for offset in offsets {
            a.read_at(&mut read_buf, 0, offset)
                .expect("failed to read record");
        }

        let duration = start_time.elapsed();

        println!("Random access read of 1000 records took: {:?}", duration);
    }

    #[test]
    fn test_rotate_functionality() {
        let temp_dir = create_temp_directory();
        let opts = Options::default();
        let aol = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Ensure there's data in the current segment to necessitate a rotation
        aol.append(b"data 1").unwrap();

        // Capture the current active_segment_id before rotation
        let current_segment_id = get_writer_state(&aol).0;

        // Perform the rotation
        aol.rotate().unwrap();

        // Verify the active_segment_id is incremented
        assert_eq!(
            get_writer_state(&aol).0,
            current_segment_id + 1,
            "Segment ID should be incremented after rotation"
        );

        // Append new data to the new segment
        let data_to_append = b"data 2";
        let (_, record_offset_start, _) = aol.append(data_to_append).unwrap();

        // Use the offset method to verify the append operation is in the new segment
        let (segment_id, current_offset) = get_writer_state(&aol);
        assert!(
            current_offset > 0,
            "Offset should be greater than 0 after appending to a new segment"
        );

        // Use read_at to verify the data integrity
        let mut read_data = vec![0u8; data_to_append.len()];
        aol.read_at(&mut read_data, segment_id, record_offset_start)
            .unwrap();
        assert_eq!(
            read_data, data_to_append,
            "Data read from the new segment should match the data appended"
        );
    }

    #[cfg(test)]
    mod fd {
        use super::*;

        #[cfg(target_os = "linux")]
        fn count_open_fds() -> std::io::Result<usize> {
            let fd_dir = format!("/proc/{}/fd", std::process::id());
            let count = std::fs::read_dir(fd_dir)?.count();
            // Subtracting 1 to account for the FD used to read the directory
            Ok(count.saturating_sub(1))
        }

        #[cfg(target_os = "macos")]
        mod macos_fd {
            use std::os::raw::{c_int, c_void};
            use std::ptr::null_mut;

            #[repr(C)]
            #[derive(Copy, Clone)]
            pub struct ProcFdInfo {
                pub proc_fd: i32,
                pub proc_fd_type: u32,
            }

            impl ProcFdInfo {
                pub fn new() -> ProcFdInfo {
                    Self {
                        proc_fd: 0,
                        proc_fd_type: 0,
                    }
                }
            }

            extern "C" {
                pub fn proc_pidinfo(
                    pid: c_int,
                    flavor: c_int,
                    arg: u64,
                    buffer: *mut c_void,
                    size: c_int,
                ) -> c_int;
            }

            pub fn count_open_fds() -> std::io::Result<usize> {
                let pid = std::process::id() as c_int;
                let fds_flavor = 1 as c_int;

                let buffer_size_bytes = unsafe { proc_pidinfo(pid, fds_flavor, 0, null_mut(), 0) };

                let fds_buffer_length: usize =
                    buffer_size_bytes as usize / std::mem::size_of::<ProcFdInfo>();
                let mut buf: Vec<ProcFdInfo> = vec![ProcFdInfo::new(); fds_buffer_length];
                buf.shrink_to_fit();

                let actual_buffer_size_bytes = unsafe {
                    proc_pidinfo(
                        pid,
                        fds_flavor,
                        0,
                        buf.as_mut_ptr() as *mut c_void,
                        buffer_size_bytes,
                    )
                };

                buf.truncate(actual_buffer_size_bytes as usize / std::mem::size_of::<ProcFdInfo>());

                Ok(buf.len())
            }
        }

        #[cfg(target_os = "macos")]
        use macos_fd::count_open_fds;

        // Test to check for file descriptor leaks
        //
        // This test runs successfully when run individually, but
        // fails when run with other tests. This is because the
        // other tests open file descriptors and it is not possible
        // to detect how many file descriptors are active as tests
        // are running concurrently.
        #[ignore]
        #[test]
        fn test_no_fd_leaks() {
            let initial_fds = count_open_fds().unwrap();

            // Test 1: Basic operations
            {
                let dir = create_temp_directory();
                let opts = Options::default();

                let aol = Aol::open(dir.path(), &opts).unwrap();
                let data = b"test data";
                aol.append(data).unwrap();

                let mut buf = vec![0u8; data.len()];
                aol.read_at(&mut buf, 0, 0).unwrap();
                aol.sync().unwrap();
            }

            #[cfg(unix)]
            {
                let fds_after_basic = count_open_fds().unwrap();
                assert_eq!(
                    initial_fds, fds_after_basic,
                    "FD leak after basic operations!"
                );
            }

            // Test 2: Multiple segments
            {
                let dir = create_temp_directory();
                let opts = Options {
                    max_file_size: 50,
                    ..Default::default()
                };

                let aol = Aol::open(dir.path(), &opts).unwrap();

                for i in 0..10 {
                    let data = vec![i as u8; 50];
                    aol.append(&data).unwrap();
                }

                let mut buf = vec![0u8; 50];
                aol.read_at(&mut buf, 0, 0).unwrap();
                aol.read_at(&mut buf, 1, 0).unwrap();
                aol.sync().unwrap();
            }

            #[cfg(unix)]
            {
                let fds_after_segments = count_open_fds().unwrap();
                assert_eq!(
                    initial_fds, fds_after_segments,
                    "FD leak after multiple segments!"
                );
            }

            // Test 3: Concurrent reads
            {
                let dir = create_temp_directory();
                let opts = Options {
                    max_file_size: 100,
                    ..Default::default()
                };

                let aol = Aol::open(dir.path(), &opts).unwrap();
                let data = vec![1u8; 100];
                let (segment_id, offset, _) = aol.append(&data).unwrap();

                let aol = Arc::new(aol);
                let mut handles = vec![];

                for _ in 0..5 {
                    let aol_clone = Arc::clone(&aol);
                    handles.push(std::thread::spawn(move || {
                        let mut buf = vec![0u8; 100];
                        for _ in 0..10 {
                            aol_clone.read_at(&mut buf, segment_id, offset).unwrap();
                        }
                    }));
                }

                for handle in handles {
                    handle.join().unwrap();
                }
            }

            #[cfg(unix)]
            {
                let fds_after_concurrent = count_open_fds().unwrap();
                assert_eq!(
                    initial_fds, fds_after_concurrent,
                    "FD leak after concurrent reads!"
                );
            }

            // Test 4: Reader pool stress
            {
                let dir = create_temp_directory();
                let opts = Options {
                    max_file_size: 50,
                    ..Default::default()
                };

                let aol = Aol::open(dir.path(), &opts).unwrap();

                for i in 0..5 {
                    let data = vec![i as u8; 50];
                    aol.append(&data).unwrap();
                }

                let aol = Arc::new(aol);
                let mut handles = vec![];

                for segment_id in 0..5 {
                    let aol_clone = Arc::clone(&aol);
                    handles.push(std::thread::spawn(move || {
                        let mut buf = vec![0u8; 50];
                        for _ in 0..100 {
                            aol_clone.read_at(&mut buf, segment_id, 0).unwrap();
                        }
                    }));
                }

                for handle in handles {
                    handle.join().unwrap();
                }
            }

            #[cfg(unix)]
            {
                let fds_after_stress = count_open_fds().unwrap();
                assert_eq!(
                    initial_fds, fds_after_stress,
                    "FD leak after reader pool stress!"
                );
            }

            // Test 5: Error handling
            {
                let dir = create_temp_directory();
                let opts = Options {
                    max_file_size: 100,
                    ..Default::default()
                };

                let aol = Aol::open(dir.path(), &opts).unwrap();

                let large_data = vec![0u8; 200];
                let _ = aol.append(&large_data); // Expected to fail

                let mut buf = vec![0u8; 10];
                let _ = aol.read_at(&mut buf, 999, 0); // Expected to fail

                for _ in 0..5 {
                    let data = vec![0u8; 90];
                    if aol.append(&data).is_err() {
                        break;
                    }
                }
            }

            #[cfg(unix)]
            {
                let final_fds = count_open_fds().unwrap();
                assert_eq!(initial_fds, final_fds, "FD leak detected in final check!");
            }
        }
    }
}
