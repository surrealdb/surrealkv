use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf}; // Import Unix-specific extensions

use crate::storage::aol::{
    Options,
    merge_slices, read_field, write_field,
    encode_record,
    CompressionFormat, CompressionLevel, Metadata, 
    BLOCK_SIZE, RECORD_HEADER_SIZE
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
    fn new(dir: &Path, id: u64, opts: &Options) -> io::Result<Self> {
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
            let header = Self::read_file_header(&mut file)?;
            file_header_offset += header.len();
        } else {
            // Create new file
            let header_len = Self::write_file_header(&mut file, id, opts)?;
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
        open_options.read(true).write(true).create(true);

        if let Some(file_mode) = opts.file_mode {
            open_options.mode(file_mode);
        }

        open_options.open(file_path)
    }

    fn read_file_header(file: &mut File) -> io::Result<Vec<u8>> {
        // Read the header using read_field
        read_field(file)
    }

    fn write_file_header(file: &mut File, id: u64, opts: &Options) -> io::Result<usize> {
        // Write the header using write_field
        let mut meta = Metadata::new_file_header(
            id,
            opts.compression_format
                .as_ref()
                .unwrap_or(&CompressionFormat::NoCompression),
            opts.compression_level
                .as_ref()
                .unwrap_or(&CompressionLevel::BestSpeed),
        );

        if let Some(metadata) = &opts.metadata {
            let buf = metadata.bytes();
            meta.read_from(&mut &buf[..]).unwrap();
        }

        let mut header = Vec::new();
        write_field(&meta.bytes(), &mut header)?;

        // Write header to the file
        file.write_all(&header)?;

        // Sync data to disk and flush metadata
        file.sync_all()?;

        Ok(header.len())
    }

    fn segment_name(index: u64, ext: &str) -> String {
        if ext.is_empty() {
            return format!("{:020}", index);
        }
        format!("{:020}.{}", index, ext)
    }

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let bytes_written = self.file.write(buf)?;
        self.file_offset += bytes_written as u64;
        Ok(bytes_written)
    }

    // Flushes the current block to disk. 
    // This method also synchronize file metadata to the filesystem
    // hence it is a bit slower than fdatasync (sync_data).
    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn close(&mut self) -> io::Result<()> {
        self.closed = true;
        self.file.sync_all()?;
        Ok(())
    }

    fn flush_block(&mut self, clear: bool) -> io::Result<()> {
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
    fn offset(&self) -> u64 {
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
    fn append(&mut self, mut rec: &[u8]) -> io::Result<usize> {
        // If the segment is closed, return an error
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Segment is closed"));
        }

        if rec.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, "buf is empty"));
        }

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

            i+=1;
        }

        // Write the remaining data to the block
        if self.block.alloc > 0 {
            self.flush_block(false)?;
        }

        Ok(n)
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
    fn read_at(&mut self, bs: &mut [u8], off: u64) -> io::Result<usize> {
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
                let buf = &mut self.block.buf
                    [self.block.alloc + boff..self.block.alloc + boff + read_chunk_size];
                merge_slices(buf, bs);
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
        self.sync().ok();
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
    fn test_append() {
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        let opts = Options::default();
        let mut a = Segment::new(&temp_dir.path(), 0, &opts).expect("should create segment");

        let sz = a.offset();
        assert_eq!(0, sz);

        let r = a.append(&[]);
        assert!(r.is_err());

        let r = a.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap());

        let r = a.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());
        assert_eq!(7, r.unwrap());

        let r = a.sync();
        assert!(r.is_ok());

        let mut bs = vec![0; 12];
        let n = a.read_at(&mut bs, 0).expect("should read");
        assert_eq!(12, n);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[RECORD_HEADER_SIZE..]);

        let mut bs = vec![0; 15];
        let n = a.read_at(&mut bs, 12).expect("should read");
        assert_eq!(15, n);
        assert_eq!(&[4,5,6, 7, 8, 9, 10].to_vec(), &bs[RECORD_HEADER_SIZE..]);

        let r = a.read_at(&mut bs, 1000);
        assert!(r.is_err());

        // Cleanup: Drop the temp directory, which deletes its contents
        drop(temp_dir);
    }
}
