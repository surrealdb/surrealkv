use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::RwLock;

use crate::storage::wal::reader::{MultiSegmentReader, Reader};
use crate::storage::WAL_RECORD_HEADER_SIZE;
use crate::storage::{get_segment_range, Options, Segment, SegmentRef};

/// Write-Ahead Log (WAL) is a data structure used to sequentially store records
/// in a series of segments. It provides efficient write operations,
/// making it suitable for use cases like write-ahead logging.
pub struct WAL {
    /// The currently active segment where data is being written.
    active_segment: Segment,

    /// The ID of the currently active segment.
    active_segment_id: u64,

    /// The directory where the segment files are located.
    dir: PathBuf,

    /// Configuration options for the WAL instance.
    opts: Options,

    /// A flag indicating whether the WAL instance is closed or not.
    closed: bool,

    /// A read-write lock used to synchronize concurrent access to the WAL instance.
    mutex: RwLock<()>, // TODO: Lock only the active segment
}

impl WAL {
    /// Opens or creates a new WAL instance associated with the specified directory and segment ID.
    ///
    /// This function prepares the WAL instance by creating the necessary directory,
    /// determining the active segment ID, and initializing the active segment.
    ///
    /// # Parameters
    ///
    /// - `dir`: The directory where segment files are located.
    /// - `opts`: Configuration options for the WAL instance.
    pub fn open(dir: &Path, opts: &Options) -> io::Result<Self> {
        // Ensure the options are valid
        opts.validate()?;

        // Ensure the directory exists with proper permissions
        Self::prepare_directory(dir, opts)?;

        // Determine the active segment ID
        let active_segment_id = Self::calculate_active_segment_id(dir)?;

        // Open the active segment
        let active_segment = Segment::open(dir, active_segment_id, opts)?;

        Ok(Self {
            active_segment,
            active_segment_id,
            dir: dir.to_path_buf(),
            opts: opts.clone(),
            closed: false,
            mutex: RwLock::new(()),
        })
    }

    // Helper function to prepare the directory with proper permissions
    fn prepare_directory(dir: &Path, opts: &Options) -> io::Result<()> {
        fs::create_dir_all(dir)?;

        if let Ok(metadata) = fs::metadata(dir) {
            let mut permissions = metadata.permissions();
            permissions.set_mode(opts.dir_mode.unwrap_or(0o750));
            fs::set_permissions(dir, permissions)?;
        }

        Ok(())
    }

    // Helper function to calculate the active segment ID
    fn calculate_active_segment_id(dir: &Path) -> io::Result<u64> {
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
    pub fn append(&mut self, rec: &[u8]) -> io::Result<(u64, usize)> {
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Segment is closed"));
        }

        if rec.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, "buf is empty"));
        }

        let _lock = self.mutex.write().unwrap();

        // Get options and initialize variables
        let opts = &self.opts;
        let mut offset = 0;

        // Calculate available space in the active segment
        let available = opts.max_file_size - self.active_segment.offset();

        // The WAL record header size is added to the record length to account for the
        // header that is prepended to each record when it is written to the segment.
        let rec_len = rec.len() as u64 + WAL_RECORD_HEADER_SIZE as u64;

        // If space is not available, create a new segment
        if rec_len > available {
            // Rotate to a new segment

            // Sync and close the active segment
            self.active_segment.close()?;

            // Update the active segment id and create a new segment
            self.active_segment_id += 1;
            let new_segment = Segment::open(&self.dir, self.active_segment_id, &self.opts)?;
            self.active_segment = new_segment;
        }

        let (off, _) = self.active_segment.append(rec)?;
        offset = off + self.calculate_offset();

        Ok((offset, rec.len() + WAL_RECORD_HEADER_SIZE))
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
    pub fn read_at(&self, buf: &mut [u8], off: u64) -> io::Result<usize> {
        if buf.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, "Buffer is empty"));
        }

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
    ) -> io::Result<usize> {
        if segment_id == self.active_segment_id {
            self.active_segment.read_at(buf, read_offset)
        } else {
            let segment = Segment::open(&self.dir, segment_id, &self.opts)?;
            segment.read_at(buf, read_offset)
        }
    }

    pub fn close(&mut self) -> io::Result<()> {
        let _lock = self.mutex.write().unwrap();
        self.active_segment.close()?;
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        let _lock = self.mutex.write().unwrap();
        self.active_segment.sync()?;
        Ok(())
    }

    // Returns the current offset within the segment.
    pub fn offset(&self) -> u64 {
        let _lock = self.mutex.read().unwrap();
        self.active_segment.offset()
    }

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
    /// Returns an `io::Result` indicating the success of the repair operation.
    pub fn repair(
        &mut self,
        corrupted_segment_id: u64,
        corrupted_offset_marker: u64,
    ) -> io::Result<()> {
        // Read the list of segments from the directory
        let segs = SegmentRef::read_segments_from_directory(&self.dir)?;

        // Prepare to store information about the corrupted segment
        let mut corrupted_segment_info = None;

        // Loop through the segments
        for s in &segs {
            // Close the active segment if its ID matches
            if s.id == self.active_segment_id {
                self.active_segment.close()?;
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
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Corrupted segment not found",
            ));
        }

        // Retrieve the information about the corrupted segment
        let (corrupted_segment_path, corrupted_segment_file_header_offset) =
            corrupted_segment_info.unwrap();

        // Prepare the repaired segment path
        let repaired_segment_path = corrupted_segment_path.with_extension("repair");

        // Rename the corrupted segment to the repaired segment
        std::fs::rename(&corrupted_segment_path, &repaired_segment_path)?;

        // Open a new segment as the active segment
        let new_segment = Segment::open(&self.dir, corrupted_segment_id, &self.opts)?;
        self.active_segment = new_segment;
        self.active_segment_id = corrupted_segment_id;

        // Create a segment reader for the repaired segment
        let segments: Vec<SegmentRef> = vec![SegmentRef {
            file_path: repaired_segment_path.clone(),
            file_header_offset: corrupted_segment_file_header_offset,
            id: corrupted_segment_id,
        }];
        let segment_reader = MultiSegmentReader::new(segments)?;

        // Initialize a reader for the segment
        let mut reader = Reader::new(segment_reader);

        // Read records until the offset marker is reached
        while let Ok((data, cur_offset)) = reader.read() {
            if cur_offset >= corrupted_offset_marker {
                break;
            }

            self.append(data)?;
        }

        // Flush and close the active segment
        self.active_segment.close()?;

        // Remove the repaired segment file
        std::fs::remove_file(&repaired_segment_path)?;

        // Open the next segment and make it active
        self.active_segment_id += 1;
        let new_segment = Segment::open(&self.dir, self.active_segment_id, &self.opts)?;
        self.active_segment = new_segment;

        Ok(())
    }
}

impl Drop for WAL {
    /// Attempt to fsync data on drop, in case we're running without sync.
    fn drop(&mut self) {
        self.close().ok();
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::WAL_RECORD_HEADER_SIZE;

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
        let opts = Options::default().with_wal();
        let mut a = WAL::open(temp_dir.path(), &opts).expect("should create aol");

        // Test initial offset
        let sz = a.offset();
        assert_eq!(0, sz);

        // Test appending an empty buffer
        let r = a.append(&[]);
        assert!(r.is_err());

        // Test appending a non-empty buffer
        let r = a.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(11, r.unwrap().1);

        // Test appending another buffer
        let r = a.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());
        assert_eq!(14, r.unwrap().1);

        // Validate offset after appending
        // 7 + 4 + 7 + 7 = 25
        assert_eq!(a.offset(), 25);

        // Test syncing segment
        let r = a.sync();
        assert!(r.is_ok());

        // Validate offset after syncing
        assert_eq!(a.offset(), 4096);

        // Test reading from segment
        let mut bs = vec![0; 11];
        let n = a.read_at(&mut bs, 0).expect("should read");
        assert_eq!(11, n);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        // Test reading another portion of data from segment
        let mut bs = vec![0; 14];
        let n = a.read_at(&mut bs, 11).expect("should read");
        assert_eq!(14, n);
        assert_eq!(
            &[4, 5, 6, 7, 8, 9, 10].to_vec(),
            &bs[WAL_RECORD_HEADER_SIZE..]
        );

        // Test reading beyond segment's current size
        let mut bs = vec![0; 14];
        let r = a.read_at(&mut bs, 4097);
        assert!(r.is_err());

        // Test appending another buffer after syncing
        let r = a.append(&[11, 12, 13, 14]);
        assert!(r.is_ok());
        assert_eq!(11, r.unwrap().1);

        // Validate offset after appending
        // 4096 + 7 + 4 = 4107
        assert_eq!(a.offset(), 4107);

        // Test reading from segment after appending
        let mut bs = vec![0; 11];
        let n = a.read_at(&mut bs, 4096).expect("should read");
        assert_eq!(11, n);
        assert_eq!(&[11, 12, 13, 14].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        // Test syncing segment again
        let r = a.sync();
        assert!(r.is_ok());

        // Validate offset after syncing again
        assert_eq!(a.offset(), 4096 * 2);

        // Test closing wal
        assert!(a.close().is_ok());
    }

    #[test]
    fn test_wal_reopen() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options::default().with_wal();
        let mut a = WAL::open(temp_dir.path(), &opts).expect("should create aol");

        // Test appending a non-empty buffer
        let r = a.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(11, r.unwrap().1);

        // Test closing wal
        assert!(a.close().is_ok());

        // Reopen the wal
        let mut a = WAL::open(temp_dir.path(), &opts).expect("should open aol");

        // Test appending another buffer
        let r = a.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());
        assert_eq!(14, r.unwrap().1);

        // Validate offset after appending
        // 4096 + 7 + 7 = 4110
        assert_eq!(a.offset(), 4110);

        // Test reading from segment
        let mut bs = vec![0; 11];
        let n = a.read_at(&mut bs, 0).expect("should read");
        assert_eq!(11, n);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        // Test reading another portion of data from segment
        let mut bs = vec![0; 14];
        let n = a.read_at(&mut bs, 4096).expect("should read");
        assert_eq!(14, n);
        assert_eq!(
            &[4, 5, 6, 7, 8, 9, 10].to_vec(),
            &bs[WAL_RECORD_HEADER_SIZE..]
        );

        // Test reading beyond segment's current size
        let mut bs = vec![0; 14];
        let r = a.read_at(&mut bs, 4097);
        assert!(r.is_err());

        // Test appending another buffer after syncing
        let r = a.append(&[11, 12, 13, 14]);
        assert!(r.is_ok());
        assert_eq!(11, r.unwrap().1);

        // Validate offset after appending
        // 4110 + 7 + 4 = 4121
        assert_eq!(a.offset(), 4121);

        // Test reading from segment after appending
        let mut bs = vec![0; 11];
        let n = a.read_at(&mut bs, 4110).expect("should read");
        assert_eq!(11, n);
        assert_eq!(&[11, 12, 13, 14].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        // Test closing wal
        assert!(a.close().is_ok());
    }
}
