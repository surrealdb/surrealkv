use std::fs;
use std::io;
use std::mem;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;

use lru::LruCache;
use parking_lot::RwLock;
use std::num::NonZeroUsize;

use crate::storage::log::{get_segment_range, Error, IOError, Options, Result, Segment};

const RECORD_HEADER_SIZE : usize = 0;

/// Append-Only Log (AOL) is a data structure used to sequentially store records
/// in a series of segments. It provides efficient write operations,
/// making it suitable for use cases like storing large amounts of data and
/// writing data in a sequential manner. This is useful for applications which
/// need to store large amounts of data separately in a variety of files, and
/// store only the offsets of the files in the main data structure. Useful for
/// WISCKEY implementation where we need to store the key-value pairs in a
/// separate file and store only the offsets in the main data structure.
pub struct AOL {
    /// The currently active segment where data is being written.
    current_write_segment: Segment<RECORD_HEADER_SIZE>,

    /// The ID of the currently active segment.
    current_write_segment_id: u64,

    /// The directory where the segment files are located.
    dir: PathBuf,

    /// Configuration options for the AOL instance.
    opts: Options,

    /// A flag indicating whether the AOL instance is closed or not.
    closed: bool,

    /// A read-write lock used to synchronize concurrent access to the AOL instance.
    mutex: RwLock<()>,

    /// A cache used to store recently used segments to avoid opening and closing the files.
    segment_cache: RwLock<LruCache<u64, Segment<RECORD_HEADER_SIZE>>>,
}

impl AOL {
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
        let current_write_segment_id = Self::calculate_current_write_segment_id(dir)?;

        // Open the active segment
        let current_write_segment = Segment::open(dir, current_write_segment_id, opts)?;

        // Create the segment cache
        // TODO: fix unwrap and return error
        let cache = LruCache::new(NonZeroUsize::new(opts.max_open_files).unwrap());

        Ok(Self {
            current_write_segment,
            current_write_segment_id,
            dir: dir.to_path_buf(),
            opts: opts.clone(),
            closed: false,
            mutex: RwLock::new(()),
            segment_cache: RwLock::new(cache),
        })
    }

    // Helper function to prepare the directory with proper permissions
    fn prepare_directory(dir: &Path, opts: &Options) -> Result<()> {
        fs::create_dir_all(dir)?;

        if let Ok(metadata) = fs::metadata(dir) {
            let mut permissions = metadata.permissions();
            permissions.set_mode(opts.dir_mode.unwrap_or(0o750));
            fs::set_permissions(dir, permissions)?;
        }

        Ok(())
    }

    // Helper function to calculate the active segment ID
    fn calculate_current_write_segment_id(dir: &Path) -> Result<u64> {
        let (_, last) = get_segment_range(dir)?;
        Ok(if last > 0 { last + 1 } else { 0 })
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

        if rec.is_empty() {
            return Err(Error::EmptyBuffer);
        }

        let _lock = self.mutex.write();

        // Get options and initialize variables
        let opts = &self.opts;
        let mut n = 0usize;
        let mut offset = 0;

        // TODO: check if there is a potential infinite loop here if the
        // record is larger than the max file size. Write a test to check this.
        while n < rec.len() {
            // Calculate available space in the active segment
            let available = opts.max_file_size as i64 - self.current_write_segment.offset() as i64;

            // If space is not available, create a new segment
            if available <= 0 {
                // Rotate to a new segment

                // Sync and close the active segment
                // Note that closing the segment will
                // not close the underlying file until
                // it is dropped.
                self.current_write_segment.close()?;

                // Increment the active segment id
                self.current_write_segment_id += 1;

                // Open a new segment for writing
                let new_segment =
                    Segment::open(&self.dir, self.current_write_segment_id, &self.opts)?;

                // Retrieve the previous active segment and replace it with the new one
                let previous_segment_id = self.current_write_segment_id - 1;
                let previous_segment = mem::replace(&mut self.current_write_segment, new_segment);

                // Cache the previous segment
                let mut cache = self.segment_cache.write();
                cache.put(previous_segment_id, previous_segment);
            }

            // Calculate the amount of data to append
            let d = std::cmp::min(available as usize, rec.len() - n);
            let (off, _) = self.current_write_segment.append(&rec[n..n + d])?;

            // Calculate offset only for the first chunk of data
            if n == 0 {
                offset = off + self.calculate_offset();
            }

            n += d;
        }

        Ok((offset, n))
    }

    // Helper function to calculate offset
    fn calculate_offset(&self) -> u64 {
        self.current_write_segment_id * self.opts.max_file_size
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
        if buf.is_empty() {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::UnexpectedEof,
                "Buffer is empty",
            )));
        }

        let _lock = self.mutex.read();

        let mut r = 0;
        while r < buf.len() {
            let offset = off + r as u64;
            let segment_id = off / self.opts.max_file_size;
            let read_offset = offset % self.opts.max_file_size;

            // Read data from the appropriate segment
            r += self.read_segment_data(&mut buf[r..], segment_id, read_offset)?;
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
        let mut cache = self.segment_cache.write();
        let reader = cache.get(&segment_id);
        if reader.is_none() {
            let segment = Segment::open(&self.dir, segment_id, &self.opts)?;
            let read_bytes = segment.read_at(buf, read_offset)?;
            cache.put(segment_id, segment);
            Ok(read_bytes)
        } else {
            let segment = reader.unwrap();
            segment.read_at(buf, read_offset)
        }
    }

    pub fn close(&mut self) -> Result<()> {
        let _lock = self.mutex.write();
        self.current_write_segment.close()?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        let _lock = self.mutex.write();
        self.current_write_segment.sync()?;
        Ok(())
    }

    // Returns the current offset within the segment.
    pub fn offset(&self) -> Result<u64> {
        let _lock = self.mutex.read();
        Ok(self.current_write_segment.offset())
    }

    pub fn size(&self) -> Result<u64> {
        let _lock = self.mutex.read();
        let cur_segment_size = self.current_write_segment.file_offset;
        let total_size =
            (self.current_write_segment_id * self.opts.max_file_size) + cur_segment_size;
        Ok(total_size)
    }
}

impl Drop for AOL {
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
    fn test_append() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options::default();
        let mut a = AOL::open(temp_dir.path(), &opts).expect("should create aol");

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
}
