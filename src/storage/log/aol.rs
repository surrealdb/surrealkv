use std::fs;
use std::io;
use std::mem;
use std::num::NonZeroUsize;

use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use lru::LruCache;
use parking_lot::{Mutex, RwLock};

use crate::storage::log::{get_segment_range, Error, IOError, Options, Result, Segment};

/// Append-Only Log (Aol) is a data structure used to sequentially store records
/// in a series of segments. It provides efficient write operations,
/// making it suitable for use cases like storing large amounts of data and
/// writing data in a sequential manner.
pub struct Aol {
    /// The currently active segment where data is being written.
    pub(crate) active_segment: Segment,

    /// The ID of the currently active segment.
    pub(crate) active_segment_id: u64,

    /// The directory where the segment files are located.
    pub(crate) dir: PathBuf,

    /// Configuration options for the AOL instance.
    pub(crate) opts: Options,

    /// A flag indicating whether the AOL instance is closed or not.
    closed: bool,

    /// A read-write lock used to synchronize concurrent access to the AOL instance.
    mutex: Mutex<()>,

    /// A cache used to store recently used segments to avoid opening and closing the files.
    segment_cache: RwLock<LruCache<u64, Segment>>,

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
        let active_segment = Segment::open(dir, active_segment_id, opts)?;

        // Create the segment cache
        // TODO: fix unwrap and return error
        let cache = LruCache::new(NonZeroUsize::new(opts.max_open_files).unwrap());

        Ok(Self {
            active_segment,
            active_segment_id,
            dir: dir.to_path_buf(),
            opts: opts.clone(),
            closed: false,
            mutex: Mutex::new(()),
            segment_cache: RwLock::new(cache),
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
    pub fn append(&mut self, rec: &[u8]) -> Result<(u64, u64, usize)> {
        if self.closed {
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

        let _lock = self.mutex.lock();

        // Get options and initialize variables
        let opts = &self.opts;

        // Calculate available space in the active segment
        let available = opts.max_file_size as i64 - self.active_segment.offset() as i64;

        // If the entire record can't fit into the remaining space of the current segment,
        // close the current segment and create a new one
        if available < rec.len() as i64 {
            // Rotate to a new segment

            // Sync and close the active segment
            // Note that closing the segment will
            // not close the underlying file until
            // it is dropped.
            self.active_segment.close()?;

            // Increment the active segment id
            self.active_segment_id += 1;

            // Open a new segment for writing
            let new_segment = Segment::open(&self.dir, self.active_segment_id, &self.opts)?;

            // Retrieve the previous active segment and replace it with the new one
            let _ = mem::replace(&mut self.active_segment, new_segment);
        }

        // Write the record to the segment
        let result = self.active_segment.append(rec);
        let offset = match result {
            Ok(off) => off,
            Err(e) => {
                if let Error::IO(_) = e {
                    self.set_fsync_failed(true);
                }
                return Err(e);
            }
        };

        Ok((self.active_segment_id, offset, rec.len()))
    }

    /// Flushes and syncs the active segment.
    pub fn sync(&mut self) -> Result<()> {
        self.check_if_fsync_failed()?;
        self.active_segment.sync()
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

        let mut r = 0;

        // Read data from the appropriate segment
        match self.read_segment_data(&mut buf[r..], segment_id, read_offset) {
            Ok(bytes_read) => {
                r += bytes_read;
            }
            Err(e) => match e {
                Error::Eof => {
                    return Err(Error::Eof);
                }
                _ => return Err(e),
            },
        }

        Ok((segment_id, r))
    }

    // Helper function to read data from the appropriate segment
    fn read_segment_data(
        &self,
        buf: &mut [u8],
        segment_id: u64,
        read_offset: u64,
    ) -> Result<usize> {
        // During read, we acquire a lock to not allow concurrent writes and reads
        // to the active segment file to avoid seek errors.
        if segment_id == self.active_segment.id {
            let _lock = self.mutex.lock();
            self.active_segment.read_at(buf, read_offset)
        } else {
            let mut cache = self.segment_cache.write();
            match cache.get(&segment_id) {
                Some(segment) => segment.read_at(buf, read_offset),
                None => {
                    let segment = Segment::open(&self.dir, segment_id, &self.opts)?;
                    let read_bytes = segment.read_at(buf, read_offset)?;
                    cache.push(segment_id, segment);
                    Ok(read_bytes)
                }
            }
        }
    }

    pub fn close(&mut self) -> Result<()> {
        let _lock = self.mutex.lock();
        self.active_segment.close()?;
        Ok(())
    }

    pub fn rotate(&mut self) -> Result<u64> {
        let _lock = self.mutex.lock();
        self.active_segment.close()?;
        self.active_segment_id += 1;
        self.active_segment = Segment::open(&self.dir, self.active_segment_id, &self.opts)?;
        Ok(self.active_segment_id)
    }

    pub fn size(&self) -> Result<u64> {
        let _lock = self.mutex.lock();
        let cur_segment_size = self.active_segment.file_offset;
        let total_size = (self.active_segment_id * self.opts.max_file_size) + cur_segment_size;
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

    #[test]
    fn append() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options::default();
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Test initial offset
        let sz = (a.active_segment_id, a.active_segment.offset());
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

        let (segment_id, offset) = (a.active_segment_id, a.active_segment.offset());

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
        let r = a.read_at(&mut bs, segment_id, 4097);
        assert!(r.is_err());

        // Test appending another buffer
        let r = a.append(&[11, 12, 13, 14]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().2);

        let (segment_id, offset) = (a.active_segment_id, a.active_segment.offset());
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
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

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

        let (segment_id, _) = (a.active_segment_id, a.active_segment.offset());

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
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

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
        let (segment_id, _) = (a.active_segment_id, a.active_segment.offset());
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

        let (segment_id, _) = (a.active_segment_id, a.active_segment.offset());

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
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        let large_record = vec![1; 1025];
        let small_record = vec![1; 1024];
        let r = a.append(&small_record);
        assert!(r.is_ok());
        let (segment_id, offset) = (a.active_segment_id, a.active_segment.offset());
        assert_eq!(0, segment_id);
        assert_eq!(1024, offset);

        let r = a.append(&large_record);
        assert!(r.is_err());
        let (segment_id, offset) = (a.active_segment_id, a.active_segment.offset());
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
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        let small_record = vec![1; 1024];
        let r = a.append(&small_record);
        assert!(r.is_ok());
        let (segment_id, offset) = (a.active_segment_id, a.active_segment.offset());
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
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        let large_record = vec![1; 1024];
        let small_record = vec![1; 512];
        let r = a.append(&large_record);
        assert!(r.is_ok());
        let (segment_id, offset) = (a.active_segment_id, a.active_segment.offset());
        assert_eq!(0, segment_id);
        assert_eq!(1024, offset);

        assert_eq!(0, a.active_segment_id);

        a.close().expect("should close");

        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");
        assert_eq!(0, a.active_segment_id);

        let r = a.append(&small_record);
        assert!(r.is_ok());
        let (segment_id, offset) = (a.active_segment_id, a.active_segment.offset());
        assert_eq!(1, segment_id);
        assert_eq!(512, offset);
        assert_eq!(1, a.active_segment_id);
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
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Append a record that fits within the first file
        let first_record = vec![1; 512];
        a.append(&first_record)
            .expect("first append should succeed");
        let (first_segment_id, first_offset) = (a.active_segment_id, a.active_segment.offset());
        assert_eq!(0, first_segment_id);
        assert_eq!(512, first_offset);

        // Append another record that causes a new file (segment) to be created
        let second_record = vec![2; 1024]; // This will exceed the max_file_size
        a.append(&second_record)
            .expect("second append should succeed");
        let (second_segment_id, second_offset) = (a.active_segment_id, a.active_segment.offset());
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
    fn read_beyond_current_offset_should_fail() {
        // Setup: Create a temporary directory and initialize the log with default options
        let temp_dir = create_temp_directory();
        let opts = Options::default();
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Append a single record to ensure there is some data in the log
        let record = vec![1; 512];
        a.append(&record).expect("append should succeed");

        // Attempt to read beyond the current offset
        let mut read_buf = vec![0; 1024]; // Buffer size is larger than the appended record
        let result = a.read_at(&mut read_buf, 0, 1024); // Attempt to read at an offset beyond the single appended record

        // Verify: The read operation should fail or return an error indicating the offset is out of bounds
        assert!(
            result.is_err(),
            "Reading beyond the current offset should fail"
        );
    }

    #[test]
    fn append_after_reopening_log() {
        // Setup: Create a temporary directory and initialize the log
        let temp_dir = create_temp_directory();
        let opts = Options::default();
        let record = vec![1; 512];

        // Step 1: Open the log, append a record, and then close it
        {
            let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");
            a.append(&record).expect("append should succeed");
        } // Log is closed here as `a` goes out of scope

        // Step 2: Reopen the log and append another record
        {
            let mut a = Aol::open(temp_dir.path(), &opts).expect("should reopen aol");
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
            let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");
            a.append(&record).expect("first append should succeed");
            a.append(&record).expect("first append should succeed");
        } // Log is closed here as `a` goes out of scope

        // Step 2: Reopen the log and append another record, which should create a new file
        {
            let mut a = Aol::open(temp_dir.path(), &opts).expect("should reopen aol");

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
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

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
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

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
        let mut aol = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Ensure there's data in the current segment to necessitate a rotation
        aol.append(b"data 1").unwrap();

        // Capture the current active_segment_id before rotation
        let current_segment_id = aol.active_segment_id;

        // Perform the rotation
        aol.rotate().unwrap();

        // Verify the active_segment_id is incremented
        assert_eq!(
            aol.active_segment_id,
            current_segment_id + 1,
            "Segment ID should be incremented after rotation"
        );

        // Append new data to the new segment
        let data_to_append = b"data 2";
        let (_, record_offset_start, _) = aol.append(data_to_append).unwrap();

        // Use the offset method to verify the append operation is in the new segment
        let (segment_id, current_offset) = (aol.active_segment_id, aol.active_segment.offset());
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
}
