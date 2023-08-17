use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, Write};
use std::num::ParseIntError;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf}; // Import Unix-specific extensions

use crate::storage::{
    encode_record, merge_slices, read_file_header, validate_file_header, write_file_header,
    Options, BLOCK_SIZE, RECORD_HEADER_SIZE,
};

/// A `Block` is an in-memory buffer that stores data before it is flushed to disk. It is used to
/// batch writes to improve performance by reducing the number of individual disk writes. If the
/// data to be written exceeds the `BLOCK_SIZE`, it will be split and flushed separately. The `Block`
/// keeps track of the allocated space, flushed data, and other details related to the write process.
/// It can also be used to align data to the size of a direct I/O block, if applicable.
///
/// # Type Parameters
///
/// - `BLOCK_SIZE`: The size of the block in bytes.
pub(crate) struct Block<const BLOCK_SIZE: usize> {
    /// The number of bytes currently allocated in the block.
    alloc: usize,

    /// The number of bytes that have been flushed to disk.
    flushed: usize,

    /// The buffer that holds the actual data.
    buf: [u8; BLOCK_SIZE],
}

impl<const BLOCK_SIZE: usize> Block<BLOCK_SIZE> {
    fn new() -> Self {
        Block {
            alloc: 0,
            flushed: 0,
            buf: [0; BLOCK_SIZE],
        }
    }

    fn remaining(&self) -> usize {
        BLOCK_SIZE - self.alloc - RECORD_HEADER_SIZE
    }

    fn is_full(&self) -> bool {
        BLOCK_SIZE - self.alloc <= RECORD_HEADER_SIZE
    }

    fn reset(&mut self) {
        self.buf = [0u8; BLOCK_SIZE];
        self.alloc = 0;
        self.flushed = 0;
    }

    fn unwritten(&self) -> usize {
        self.alloc - self.flushed
    }
}

/// Represents a segment in a write-ahead log.
///
/// A `Segment` represents a portion of the write-ahead log. It holds information about the file
/// that stores the log entries, as well as details related to the segment's data and state.
///
/// A segment header is stored at the beginning of the segment file. It has the following format:
///
///
///   File Header
///
///     0      1      2      3      4      5      6      7      8
///     +------+------+------+------+------+------+------+------+
///     | Magic                                                 |
///     +------+------+------+------+------+------+------+------+
///     | Version                                               |
///     +------+------+------+------+------+------+------+------+
///     | SegmentID                                             |
///     +------+------+------+------+------+------+------+------+
///     | Compression                                           |
///     +------+------+------+------+------+------+------+------+
///     | Compression Level                                     |
///     +------+------+------+------+------+------+------+------+
///     | Metadata                                              |
///     +------+------+------+------+------+------+------+------+
///     .                                                       |
///     .                                                       |
///     .                                                       |
///     +------+------+------+------+------+------+------+------+
///
pub(crate) struct Segment {
    /// The unique identifier of the segment.
    id: u64,

    /// The directory where the segment file is located.
    dir: PathBuf,

    /// The active block for buffering data.
    block: Block<BLOCK_SIZE>,

    /// The number of blocks that have been written to the segment.
    written_blocks: usize,

    /// The underlying file for storing the segment's data.
    file: File,

    /// The base offset of the file.
    file_header_offset: u64,

    /// The current offset within the file.
    file_offset: u64,

    /// A flag indicating whether the segment is closed or not.
    closed: bool,
}

impl Segment {
    pub(crate) fn open(dir: &Path, id: u64, opts: &Options) -> io::Result<Self> {
        // Ensure the options are valid
        opts.validate()?;

        // Build the file path using the segment name and extension
        let extension = opts.extension.as_deref().unwrap_or("");
        let file_path = dir.join(Self::segment_name(id, extension));
        let file_path_exists = file_path.exists();
        let file_path_is_file = file_path.is_file();

        // Open the file with the specified options
        let mut file = Self::open_file(&file_path, opts)?;

        // Initialize the file header offset
        let mut file_header_offset = 0;

        // If the file already exists
        if file_path_exists && file_path_is_file {
            // Handle existing file
            let header = read_file_header(&mut file)?;
            validate_file_header(&header, id, opts)?;

            file_header_offset += (4 + header.len());
            // TODO: take id etc from existing header
        } else {
            // Write new file header
            let header_len = write_file_header(&mut file, id, opts)?;
            file_header_offset += header_len;
        }

        // Seek to the end of the file to get the file offset
        let file_offset = file.seek(io::SeekFrom::End(0))?;

        // Initialize and return the Segment
        Ok(Segment {
            file,
            file_header_offset: file_header_offset as u64,
            file_offset: file_offset - file_header_offset as u64,
            dir: dir.to_path_buf(),
            id,
            closed: false,
            written_blocks: 0,
            block: Block::new(),
        })
    }

    fn open_file(file_path: &Path, opts: &Options) -> io::Result<File> {
        let mut open_options = OpenOptions::new();
        open_options.read(true).write(true);

        if let Some(file_mode) = opts.file_mode {
            open_options.mode(file_mode);
        }

        if !file_path.exists() {
            open_options.create(true); // Create the file if it doesn't exist
        }

        open_options.open(file_path)
    }

    pub(crate) fn segment_name(index: u64, ext: &str) -> String {
        if ext.is_empty() {
            return format!("{:020}", index);
        }
        format!("{:020}.{}", index, ext)
    }

    pub(crate) fn parse_segment_name(name: &str) -> io::Result<(u64, Option<String>)> {
        let parts: Vec<&str> = name.split('.').collect();

        if parts.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid segment name format",
            ));
        }

        let index: Result<u64, ParseIntError> = parts[0].parse();
        if let Ok(index) = index {
            if parts.len() == 1 {
                return Ok((index, None));
            } else if parts.len() == 2 {
                return Ok((index, Some(parts[1].to_string())));
            }
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Invalid segment name format",
        ))
    }

    // Flushes the current block to disk.
    // This method also synchronize file metadata to the filesystem
    // hence it is a bit slower than fdatasync (sync_data).
    pub(crate) fn sync(&mut self) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Segment is closed"));
        }

        if self.block.alloc > 0 {
            self.flush_block(true)?;
        }
        self.file.sync_all()
    }

    pub(crate) fn close(&mut self) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Segment is closed"));
        }
        self.sync()?;
        self.closed = true;
        Ok(())
    }

    pub(crate) fn flush_block(&mut self, clear: bool) -> io::Result<()> {
        let mut p = &mut self.block;
        let clear = clear || p.is_full();

        // No more data will fit into the block or an implicit clear.
        // Enqueue and clear it.
        if clear {
            p.alloc = BLOCK_SIZE; // Write till end of block.
        }

        let n = self.file.write(&p.buf[p.flushed..p.alloc])?;
        p.flushed += n;
        self.file_offset += n as u64;

        // We flushed an entire block, prepare a new one.
        if clear {
            p.reset();
            self.written_blocks += 1;
        }

        Ok(())
    }

    // Returns the current offset within the segment.
    pub(crate) fn offset(&self) -> u64 {
        self.file_offset + self.block.unwritten() as u64
    }

    /// Appends data to the segment.
    ///
    /// This method appends the given data to the segment. If the block is full, it is flushed
    /// to disk. The data is written in chunks to the current block until the block is full.
    ///
    /// # Parameters
    ///
    /// - `buf`: The data to be appended.
    ///
    /// # Returns
    ///
    /// Returns the number of bytes successfully appended.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is closed.
    pub(crate) fn append(&mut self, mut rec: &[u8]) -> io::Result<(u64, usize)> {
        // If the segment is closed, return an error
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Segment is closed"));
        }

        if rec.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, "buf is empty"));
        }

        let offset = self.offset();

        // If the block is full, flush it
        if self.block.is_full() {
            self.flush_block(true)?;
        }

        let mut n = 0;
        let mut i = 0;
        while i == 0 || !rec.is_empty() {
            let p = &mut self.block;
            // Find the number of bytes that can be written to the block
            let l = std::cmp::min(p.remaining(), rec.len());
            let part = &rec[..l];
            let buf = &mut p.buf[p.alloc..]; // Create a mutable slice starting from p.alloc

            // Encode the content of 'part' into 'buf'
            encode_record(buf, rec.len(), part, i);

            p.alloc += part.len() + RECORD_HEADER_SIZE; // Update the number of bytes allocated in the block
            if p.is_full() {
                self.flush_block(true)?;
            }

            rec = &rec[l..]; // Update the remaining bytes to be written

            n += l;

            i += 1;
        }

        // Write the remaining data to the block
        if self.block.alloc > 0 {
            self.flush_block(false)?;
        }

        Ok((offset, n))
    }

    /// Reads data from the segment at the specified offset.
    ///
    /// This method reads data from the segment starting from the given offset. It reads
    /// from the underlying file if the offset is beyond the current block's buffer. The
    /// read data is then copied into the provided byte slice `bs`.
    ///
    /// # Parameters
    ///
    /// - `bs`: A byte slice to store the read data.
    /// - `off`: The offset from which to start reading.
    ///
    /// # Returns
    ///
    /// Returns the number of bytes read and any encountered error.
    ///
    /// # Errors
    ///
    /// Returns an error if the provided offset is negative or if there is an I/O error
    /// during reading.
    pub(crate) fn read_at(&self, bs: &mut [u8], off: u64) -> io::Result<usize> {
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Segment is closed"));
        }

        if off > self.offset() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Offset beyond current position",
            ));
        }

        // Calculate buffer offset
        let mut boff = 0;

        let mut n = 0;
        if off < self.file_offset {
            // Read from the file
            n = self.file.read_at(bs, self.file_header_offset + off)?;
        } else {
            boff = (off - self.file_offset) as usize;
        }

        let pending = bs.len() - n;
        if pending > 0 {
            let available = self.block.unwritten() - boff;
            let read_chunk_size = std::cmp::min(pending, available);

            if read_chunk_size > 0 {
                let buf = &self.block.buf
                    [self.block.alloc + boff..self.block.alloc + boff + read_chunk_size];
                merge_slices(bs, buf);
            }

            if read_chunk_size == pending {
                return Ok(n);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Incomplete read",
                ));
            }
        }

        Ok(n)
    }
}

impl Drop for Segment {
    /// Attempt to fsync data on drop, in case we're running without sync.
    fn drop(&mut self) {
        self.close().ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn test_remaining() {
        let block: Block<4096> = Block {
            alloc: 100,
            flushed: 0,
            buf: [0; 4096],
        };
        assert_eq!(block.remaining(), 3996 - RECORD_HEADER_SIZE);
    }

    #[test]
    fn test_is_full() {
        let block: Block<4096> = Block {
            alloc: 4096 - RECORD_HEADER_SIZE,
            flushed: 0,
            buf: [0; 4096],
        };
        assert!(block.is_full());
    }

    #[test]
    fn test_reset() {
        let mut block: Block<4096> = Block {
            alloc: 100,
            flushed: 0,
            buf: [1; 4096],
        };
        block.reset();
        assert_eq!(block.buf, [0; 4096]);
        assert_eq!(block.alloc, 0);
        assert_eq!(block.flushed, 0);
    }

    #[test]
    fn test_segment_name_with_extension() {
        let index = 42;
        let ext = "log";
        let expected = format!("{:020}.{}", index, ext);
        assert_eq!(Segment::segment_name(index, ext), expected);
    }

    #[test]
    fn test_segment_name_without_extension() {
        let index = 42;
        let expected = format!("{:020}", index);
        assert_eq!(Segment::segment_name(index, ""), expected);
    }

    #[test]
    fn test_parse_segment_name_with_extension() {
        let name = "00000000000000000042.log";
        let result = Segment::parse_segment_name(name).unwrap();
        assert_eq!(result, (42, Some("log".to_string())));
    }

    #[test]
    fn test_parse_segment_name_without_extension() {
        let name = "00000000000000000042";
        let result = Segment::parse_segment_name(name).unwrap();
        assert_eq!(result, (42, None));
    }

    #[test]
    fn test_parse_segment_name_invalid_format() {
        let name = "invalid_name";
        let result = Segment::parse_segment_name(name);
        assert!(result.is_err());
    }

    #[test]
    fn test_append() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options and open a segment
        let opts = Options::default();
        let mut segment = Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        let sz = segment.offset();
        assert_eq!(0, sz);

        // Test appending an empty buffer
        let r = segment.append(&[]);
        assert!(r.is_err());

        // Test appending a non-empty buffer
        let r = segment.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Test appending another buffer
        let r = segment.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());
        assert_eq!(7, r.unwrap().1);

        // Validate offset after appending
        // 8 + 4 + 8 + 7 = 27
        assert_eq!(segment.offset(), 27);

        // Test syncing segment
        let r = segment.sync();
        assert!(r.is_ok());

        // Validate offset after syncing
        assert_eq!(segment.offset(), 4096);

        // Test reading from segment
        let mut bs = vec![0; 12];
        let n = segment.read_at(&mut bs, 0).expect("should read");
        assert_eq!(12, n);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[RECORD_HEADER_SIZE..]);

        // Test reading another portion of data from segment
        let mut bs = vec![0; 15];
        let n = segment.read_at(&mut bs, 12).expect("should read");
        assert_eq!(15, n);
        assert_eq!(&[4, 5, 6, 7, 8, 9, 10].to_vec(), &bs[RECORD_HEADER_SIZE..]);

        // Test reading beyond segment's current size
        let mut bs = vec![0; 15];
        let r = segment.read_at(&mut bs, 4097);
        assert!(r.is_err());

        // Test appending another buffer after syncing
        let r = segment.append(&[11, 12, 13, 14]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Validate offset after appending
        // 4096 + 8 + 4 = 4108
        assert_eq!(segment.offset(), 4108);

        // Test reading from segment after appending
        let mut bs = vec![0; 12];
        let n = segment.read_at(&mut bs, 4096).expect("should read");
        assert_eq!(12, n);
        assert_eq!(&[11, 12, 13, 14].to_vec(), &bs[RECORD_HEADER_SIZE..]);

        // Test syncing segment again
        let r = segment.sync();
        assert!(r.is_ok());

        // Validate offset after syncing again
        assert_eq!(segment.offset(), 4096 * 2);

        // Test closing segment
        assert!(segment.close().is_ok());

        // Cleanup: Drop the temp directory, which deletes its contents
        drop(temp_dir);
    }

    #[test]
    fn test_reopen_empty_file() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options and open a segment
        let opts = Options::default();
        let segment = Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        assert_eq!(0, segment.offset());

        drop(segment);

        // Reopen segment should pass
        let segment = Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        assert_eq!(0, segment.offset());
    }

    #[test]
    fn test_reopen() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options and open a segment
        let opts = Options::default();
        let mut segment = Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        assert_eq!(0, segment.offset());

        // Test appending a non-empty buffer
        let r = segment.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Test appending another buffer
        let r = segment.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());
        assert_eq!(7, r.unwrap().1);

        // Validate offset after appending
        // 8 + 4 + 8 + 7 = 27
        assert_eq!(segment.offset(), 27);

        // Test syncing segment
        assert!(segment.sync().is_ok());

        // Validate offset after syncing
        assert_eq!(segment.offset(), 4096);

        // Test closing segment
        assert!(segment.close().is_ok());

        drop(segment);

        // Reopen segment
        let mut segment = Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        assert_eq!(segment.offset(), 4096);

        // Test reading from segment
        let mut bs = vec![0; 12];
        let n = segment.read_at(&mut bs, 0).expect("should read");
        assert_eq!(12, n);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[RECORD_HEADER_SIZE..]);

        // Test reading another portion of data from segment
        let mut bs = vec![0; 15];
        let n = segment.read_at(&mut bs, 12).expect("should read");
        assert_eq!(15, n);
        assert_eq!(&[4, 5, 6, 7, 8, 9, 10].to_vec(), &bs[RECORD_HEADER_SIZE..]);

        // Test reading beyond segment's current size
        let mut bs = vec![0; 15];
        let r = segment.read_at(&mut bs, 4097);
        assert!(r.is_err());

        // Test appending another buffer after syncing
        let r = segment.append(&[11, 12, 13, 14]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Validate offset after appending
        // 4096 + 8 + 4 = 4108
        assert_eq!(segment.offset(), 4108);

        // Test reading from segment after appending
        let mut bs = vec![0; 12];
        let n = segment.read_at(&mut bs, 4096).expect("should read");
        assert_eq!(12, n);
        assert_eq!(&[11, 12, 13, 14].to_vec(), &bs[RECORD_HEADER_SIZE..]);

        // Test closing segment
        assert!(segment.close().is_ok());

        // Reopen segment
        let segment = Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");
        // Test initial offset
        assert_eq!(segment.offset(), 4096 * 2);

        // Cleanup: Drop the temp directory, which deletes its contents
        drop(segment);
        drop(temp_dir);
    }

    #[test]
    fn test_open_file() {
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        let opts = Options::default();
        let file_path = temp_dir.path().join("test_file.txt");

        // Initially, the file doesn't exist
        assert!(!file_path.exists());

        // Open the file using open_file
        let file_result = Segment::open_file(&file_path, &opts);
        assert!(file_result.is_ok());

        // Now, the file should exist
        assert!(file_path.exists());

        // Clean up: Delete the temporary directory and its contents
        temp_dir.close().expect("should delete temp dir");
    }

    #[test]
    fn test_corrupted_metadata() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options
        let opts = Options::default();

        let mut segment = Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");

        // Close the segment
        segment.close().expect("should close segment");

        // Corrupt the segment's metadata by overwriting the first few bytes
        let segment_path = temp_dir.path().join("00000000000000000000");
        let mut corrupted_data = vec![0; 4];

        // Open the file for writing before writing to it
        let mut corrupted_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&segment_path)
            .expect("should open corrupted file");

        corrupted_file
            .write_all(&mut corrupted_data)
            .expect("should write corrupted data to file");

        // Attempt to reopen the segment with corrupted metadata
        let reopened_segment = Segment::open(&temp_dir.path(), 0, &opts);
        assert!(reopened_segment.is_err()); // Opening should fail due to corrupted metadata

        // Cleanup: Drop the temp directory, which deletes its contents
        drop(temp_dir);
    }

    #[test]
    fn test_closed_segment_operations() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options
        let opts = Options::default();

        // Create a new segment file and open it
        let mut segment = Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");

        // Close the segment
        segment.close().expect("should close segment");

        // Try to perform operations on the closed segment
        let r = segment.append(&[0, 1, 2, 3]);
        assert!(r.is_err()); // Appending should fail

        let r = segment.sync();
        assert!(r.is_err()); // Syncing should fail

        let mut bs = vec![0; 12];
        let n = segment.read_at(&mut bs, 0);
        assert!(n.is_err()); // Reading should fail

        // Reopen the closed segment
        let mut segment = Segment::open(&temp_dir.path(), 0, &opts).expect("should reopen segment");

        // Try to perform operations on the reopened segment
        let r = segment.append(&[4, 5, 6, 7]);
        assert!(r.is_ok()); // Appending should succeed on reopened segment

        // Cleanup: Drop the temp directory, which deletes its contents
        drop(temp_dir);
    }
}
