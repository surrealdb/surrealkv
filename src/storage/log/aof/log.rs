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

const RECORD_HEADER_SIZE: usize = 0;

/// Append-Only Log (Aol) is a data structure used to sequentially store records
/// in a series of segments. It provides efficient write operations,
/// making it suitable for use cases like storing large amounts of data and
/// writing data in a sequential manner.
pub struct Aol {
    /// The currently active segment where data is being written.
    pub(crate) active_segment: Segment<RECORD_HEADER_SIZE>,

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
    segment_cache: RwLock<LruCache<u64, Segment<RECORD_HEADER_SIZE>>>,

    /// A flag indicating whether the AOL instance has encountered an IO error or not.
    fsync_failed: AtomicBool,
}

impl Aol {
    /// Opens or creates a new AOL instance associated with the specified directory and segment ID.
    ///
    /// This function prepares the AOL instance by creating the necessary directory,
    /// determining the active segment ID, and initializing the active segment.
    ///
    /// # Parameters
    ///
    /// - `dir`: The directory where segment files are located.
    /// - `opts`: Configuration options for the AOL instance.
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
    ///
    /// This function appends the record to the active segment. If the active segment is
    /// full, a new segment will be created and the record will be appended to it.
    ///
    /// The function returns a tuple containing the offset at which the record was appended
    /// and the number of bytes written.
    ///
    /// # Arguments
    ///
    /// * `rec` - A reference to the byte slice containing the record to be appended.
    ///
    /// # Returns
    ///
    /// A result containing the tuple `(offset, bytes_written)` or an `io::Error` in case of failure.
    ///
    /// # Errors
    ///
    /// This function may return an error if the active segment is closed, the provided record
    /// is empty, or any I/O error occurs during the appending process.
    pub fn append(&mut self, rec: &[u8]) -> Result<(u64, usize)> {
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
        match result {
            Ok((off, _)) => off,
            Err(e) => {
                if let Error::IO(_) = e {
                    self.set_fsync_failed(true);
                }
                return Err(e);
            }
        };

        let (off, _) = result.unwrap();
        // Calculate offset only for the first chunk of data
        let offset = off + self.calculate_offset();

        Ok((offset, rec.len()))
    }

    /// Flushes and syncs the active segment.
    pub fn sync(&mut self) -> Result<()> {
        self.check_if_fsync_failed()?;
        self.active_segment.sync()
    }

    /// Flushes the active segment.
    pub fn flush(&mut self) -> Result<()> {
        self.check_if_fsync_failed()?;
        self.active_segment.flush()
    }

    // Helper function to calculate offset
    fn calculate_offset(&self) -> u64 {
        self.active_segment_id * self.opts.max_file_size
    }

    /// Reads data from the segment at the specified offset into the provided buffer.
    ///
    /// This function reads data from the segment's underlying storage starting at the specified
    /// offset and writes it into the provided buffer. The number of bytes read is returned.
    ///
    /// # Arguments
    ///
    /// * `buf` - A mutable reference to a byte slice that will hold the read data.
    /// * `off` - The offset from which to start reading data within the segment.
    ///
    /// # Returns
    ///
    /// A result containing the number of bytes read or an `io::Error` in case of failure.
    ///
    /// # Errors
    ///
    /// This function may return an error if the provided buffer is empty, or any I/O error occurs
    /// during the reading process.
    pub fn read_at(&self, buf: &mut [u8], off: u64) -> Result<usize> {
        self.check_if_fsync_failed()?;

        if buf.is_empty() {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::UnexpectedEof,
                "Buffer is empty",
            )));
        }

        let mut r = 0;
        while r < buf.len() {
            let offset = off + r as u64;
            let segment_id = off / self.opts.max_file_size;
            let read_offset = offset % self.opts.max_file_size;

            // Read data from the appropriate segment
            match self.read_segment_data(&mut buf[r..], segment_id, read_offset) {
                Ok(bytes_read) => {
                    r += bytes_read;
                }
                Err(e) => match e {
                    Error::Eof(n) => {
                        r = n;
                        if n > 0 {
                            continue;
                        } else {
                            return Err(Error::Eof(n));
                        }
                    }
                    _ => return Err(e),
                },
            }
        }

        Ok(r)
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

    // Returns the current offset within the segment.
    pub fn offset(&self) -> Result<u64> {
        // Lock the mutex to ensure thread safety
        let _lock = self.mutex.lock();

        // Calculate the base offset
        let base_offset = self.calculate_offset();

        // Get the offset of the active segment
        let active_segment_offset = self.active_segment.offset();

        // Add the calculated offset to the offset of the active segment
        Ok(base_offset + active_segment_offset)
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
        let sz = a.offset().unwrap();
        assert_eq!(0, sz);

        // Test appending an empty buffer
        let r = a.append(&[]);
        assert!(r.is_err());

        // Test appending a non-empty buffer
        let r = a.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Test appending another buffer
        let r = a.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());
        assert_eq!(7, r.unwrap().1);

        // Validate offset after appending
        // 4 + 7 = 11
        assert_eq!(a.offset().unwrap(), 11);

        // Validate offset after syncing
        assert_eq!(a.offset().unwrap(), 11);

        // Test reading from segment
        let mut bs = vec![0; 4];
        let n = a.read_at(&mut bs, 0).expect("should read");
        assert_eq!(4, n);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[..]);

        // Test reading another portion of data from segment
        let mut bs = vec![0; 7];
        let n = a.read_at(&mut bs, 4).expect("should read");
        assert_eq!(7, n);
        assert_eq!(&[4, 5, 6, 7, 8, 9, 10].to_vec(), &bs[..]);

        // Test reading beyond segment's current size
        let mut bs = vec![0; 15];
        let r = a.read_at(&mut bs, 4097);
        assert!(r.is_err());

        // Test appending another buffer
        let r = a.append(&[11, 12, 13, 14]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Validate offset after appending
        // 11 + 4 = 15
        assert_eq!(a.offset().unwrap(), 15);

        // Test reading from segment after appending
        let mut bs = vec![0; 4];
        let n = a.read_at(&mut bs, 11).expect("should read");
        assert_eq!(4, n);
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
        assert_eq!(31 * 1024, r1.unwrap().1);

        // Append the second data slice to the aol
        let r2 = a.append(&data2);
        assert!(r2.is_ok());
        assert_eq!(2 * 1024, r2.unwrap().1);

        // Read the first data slice back from the aol
        let mut read_data1 = vec![0; 31 * 1024];
        let n1 = a.read_at(&mut read_data1, 0).expect("should read");
        assert_eq!(31 * 1024, n1);
        assert_eq!(data1, read_data1);

        // Read the second data slice back from the aol
        let mut read_data2 = vec![0; 2 * 1024];
        let n2 = a.read_at(&mut read_data2, 31 * 1024).expect("should read");
        assert_eq!(2 * 1024, n2);
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
        assert_eq!(31 * 1024, r1.unwrap().1);

        // Append the second data slice to the aol
        let r2 = a.append(&data2);
        assert!(r2.is_ok());
        assert_eq!(2 * 1024, r2.unwrap().1);

        // Read the first data slice back from the aol
        let mut read_data1 = vec![0; 31 * 1024];
        let n1 = a.read_at(&mut read_data1, 0).expect("should read");
        assert_eq!(31 * 1024, n1);
        assert_eq!(data1, read_data1);

        // Append the third data slice to the aol
        let r3 = a.append(&data4);
        assert!(r3.is_ok());
        assert_eq!(1024, r3.unwrap().1);

        // Append the third data slice to the aol
        let r4 = a.append(&data3);
        assert!(r4.is_ok());
        assert_eq!(1024, r4.unwrap().1);

        // Read the first data slice back from the aol
        let mut read_data1 = vec![0; 31 * 1024];
        let n1 = a.read_at(&mut read_data1, 0).expect("should read");
        assert_eq!(31 * 1024, n1);
        assert_eq!(data1, read_data1);

        // Read the second data slice back from the aol
        let mut read_data2 = vec![0; 2 * 1024];
        let n2 = a.read_at(&mut read_data2, 31 * 1024).expect("should read");
        assert_eq!(2 * 1024, n2);
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
        assert_eq!(1024, a.offset().unwrap());

        let r = a.append(&large_record);
        assert!(r.is_err());
        assert_eq!(1024, a.offset().unwrap());
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
        assert_eq!(1024, a.offset().unwrap());

        // Simulate fsync failure
        a.set_fsync_failed(true);

        // Writes should fail aftet fsync failure
        let r = a.append(&small_record);
        assert!(r.is_err());

        // Reads should fail after fsync failure
        let mut read_data = vec![0; 1024];
        let r = a.read_at(&mut read_data, 0);
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
        assert_eq!(1024, a.offset().unwrap());

        assert_eq!(0, a.active_segment_id);

        a.close().expect("should close");

        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");
        assert_eq!(0, a.active_segment_id);

        let r = a.append(&small_record);
        assert!(r.is_ok());
        assert_eq!(1536, a.offset().unwrap());
        assert_eq!(1, a.active_segment_id);
    }
}
