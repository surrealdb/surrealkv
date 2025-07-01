use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use parking_lot::RwLock;

use crate::wal::segment::{
    get_segment_range, Error, IOError, Options, Result, Segment, WAL_RECORD_HEADER_SIZE,
};

/// Write-Ahead Log (Wal) is a data structure used to sequentially
/// store records in a series of segments.
pub struct Wal {
    /// The currently active segment where data is being written.
    active_segment: Segment<WAL_RECORD_HEADER_SIZE>,

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

impl Wal {
    /// Opens or creates a new WAL instance associated with the specified directory and segment ID.
    ///
    /// This function prepares the WAL instance by creating the necessary directory,
    /// determining the active segment ID, and initializing the active segment.
    ///
    /// # Parameters
    ///
    /// - `dir`: The directory where segment files are located.
    /// - `opts`: Configuration options for the WAL instance.
    pub fn open(dir: &Path, opts: Options) -> Result<Self> {
        // Ensure the options are valid
        opts.validate()?;

        // Ensure the directory exists with proper permissions
        Self::prepare_directory(dir, &opts)?;

        // Determine the active segment ID
        let active_segment_id = Self::calculate_active_segment_id(dir)?;

        // Open the active segment
        let active_segment = Segment::open(dir, active_segment_id, &opts)?;

        Ok(Self {
            active_segment,
            active_segment_id,
            dir: dir.to_path_buf(),
            opts,
            closed: false,
            mutex: RwLock::new(()),
        })
    }

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

    fn calculate_active_segment_id(dir: &Path) -> Result<u64> {
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
    pub fn append(&mut self, rec: &[u8]) -> Result<u64> {
        if self.closed {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::Other,
                "Segment is closed",
            )));
        }

        if rec.is_empty() {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::Other,
                "buf is empty",
            )));
        }

        let _lock = self.mutex.write();

        let (off, _) = self.active_segment.append(rec)?;

        Ok(off)
    }

    #[cfg(test)]
    fn read_at(&self, buf: &mut [u8], off: u64) -> Result<usize> {
        if buf.is_empty() {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::Other,
                "Buffer is empty",
            )));
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

    #[cfg(test)]
    fn read_segment_data(
        &self,
        buf: &mut [u8],
        segment_id: u64,
        read_offset: u64,
    ) -> Result<usize> {
        if segment_id == self.active_segment_id {
            self.active_segment.read_at(buf, read_offset)
        } else {
            let segment: Segment<WAL_RECORD_HEADER_SIZE> =
                Segment::open(&self.dir, segment_id, &self.opts)?;
            segment.read_at(buf, read_offset)
        }
    }

    pub fn close(&mut self) -> Result<()> {
        let _lock = self.mutex.write();
        self.active_segment.close()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn sync(&mut self) -> Result<()> {
        let _lock = self.mutex.write();
        self.active_segment.sync()?;
        Ok(())
    }

    // Returns the current offset within the segment.
    #[cfg(test)]
    fn offset(&self) -> u64 {
        let _lock = self.mutex.read();
        self.active_segment.offset()
    }

    pub fn get_dir_path(&self) -> &Path {
        &self.dir
    }

    /// Explicitly rotates the active WAL segment to a new one.
    ///
    /// This method should be called when a memtable is being flushed to ensure
    /// a one-to-one relationship between memtables and WAL segments.
    ///
    /// # Returns
    ///
    /// The ID of the newly created segment, or an error if something went wrong.
    pub fn rotate(&mut self) -> Result<u64> {
        let _lock = self.mutex.write();

        // Sync and close the current active segment
        self.active_segment.close()?;

        // Update the active segment id and create a new segment
        self.active_segment_id += 1;
        let new_segment = Segment::open(&self.dir, self.active_segment_id, &self.opts)?;
        self.active_segment = new_segment;

        Ok(self.active_segment_id)
    }
}

impl Drop for Wal {
    /// Attempt to fsync data on drop, in case we're running without sync.
    fn drop(&mut self) {
        self.close().ok();
    }
}

#[cfg(test)]
mod tests {
    use crate::wal::segment::{BLOCK_SIZE, WAL_RECORD_HEADER_SIZE};

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
        let mut a = Wal::open(temp_dir.path(), opts).expect("should create aol");

        // Test initial offset
        let sz = a.offset();
        assert_eq!(0, sz);

        // Test appending an empty buffer
        let r = a.append(&[]);
        assert!(r.is_err());

        // Test appending a non-empty buffer
        let r = a.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());

        // Test appending another buffer
        let r = a.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());

        // Validate offset after appending
        // 7 + 4 + 7 + 7 = 25
        assert_eq!(a.offset(), 25);

        // Test syncing segment
        let r = a.sync();
        assert!(r.is_ok());

        // Validate offset after syncing
        assert_eq!(a.offset(), BLOCK_SIZE as u64);

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
        let r = a.read_at(&mut bs, BLOCK_SIZE as u64 + 1);
        assert!(r.is_err());

        // Test appending another buffer after syncing
        let r = a.append(&[11, 12, 13, 14]);
        assert!(r.is_ok());

        // Validate offset after appending
        // BLOCK_SIZE + 7 + 4 = 4107
        assert_eq!(a.offset(), BLOCK_SIZE as u64 + 7 + 4);

        // Test reading from segment after appending
        let mut bs = vec![0; 11];
        let n = a.read_at(&mut bs, BLOCK_SIZE as u64).expect("should read");
        assert_eq!(11, n);
        assert_eq!(&[11, 12, 13, 14].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        // Test syncing segment again
        let r = a.sync();
        assert!(r.is_ok());

        // Validate offset after syncing again
        assert_eq!(a.offset(), BLOCK_SIZE as u64 * 2);

        // Test closing wal
        assert!(a.close().is_ok());
    }

    #[test]
    fn wal_reopen() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options::default();
        let mut a = Wal::open(temp_dir.path(), opts).expect("should create aol");

        // Test appending a non-empty buffer
        let r = a.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());

        // Test closing wal
        assert!(a.close().is_ok());

        // Reopen the wal
        let opts = Options::default();
        let mut a = Wal::open(temp_dir.path(), opts).expect("should open aol");

        // Test appending another buffer
        let r = a.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());

        // Validate offset after appending
        // 4096 + 7 + 7 = 4110
        assert_eq!(a.offset(), BLOCK_SIZE as u64 + 7 + 7);

        // Test reading from segment
        let mut bs = vec![0; 11];
        let n = a.read_at(&mut bs, 0).expect("should read");
        assert_eq!(11, n);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        // Test reading another portion of data from segment
        let mut bs = vec![0; 14];
        let n = a.read_at(&mut bs, BLOCK_SIZE as u64).expect("should read");
        assert_eq!(14, n);
        assert_eq!(
            &[4, 5, 6, 7, 8, 9, 10].to_vec(),
            &bs[WAL_RECORD_HEADER_SIZE..]
        );

        // Test reading beyond segment's current size
        let mut bs = vec![0; 14];
        let r = a.read_at(&mut bs, BLOCK_SIZE as u64 + 1);
        assert!(r.is_err());

        // Test appending another buffer after syncing
        let r = a.append(&[11, 12, 13, 14]);
        assert!(r.is_ok());

        // Validate offset after appending
        // BLOCK_SZIE + 14 + 7 + 4 = 4121
        assert_eq!(a.offset(), BLOCK_SIZE as u64 + 14 + 7 + 4);

        // Test reading from segment after appending
        let mut bs = vec![0; 11];
        let n = a
            .read_at(&mut bs, BLOCK_SIZE as u64 + 14)
            .expect("should read");
        assert_eq!(11, n);
        assert_eq!(&[11, 12, 13, 14].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        // Test closing wal
        assert!(a.close().is_ok());
    }
}
