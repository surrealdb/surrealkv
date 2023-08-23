use std::fs::File;
use std::io::BufReader;
use std::io::{self, BufRead, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::vec::Vec;

use crate::storage::BLOCK_SIZE;

struct SegmentRef {
    /// The path where the segment file is located.
    pub(crate) file_path: PathBuf,
    /// The base offset of the file.
    pub(crate) file_header_offset: u64,
}

pub struct MultiSegmentReader {
    buf: BufReader<File>,
    segs: Vec<SegmentRef>,
    cur: usize, // Index into segs.
    off: usize, // Offset of read data into current segment.
}

impl MultiSegmentReader {
    fn new(segs: Vec<SegmentRef>) -> Result<MultiSegmentReader, Box<dyn std::error::Error>> {
        if segs.is_empty() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Empty segment list",
            )));
        }

        let cur = 0;
        let off = 0;

        // Open the first segment's file for reading
        let mut file = File::open(&segs[cur].file_path)?;
        file.seek(SeekFrom::Start(segs[cur].file_header_offset))?;

        let buf = BufReader::new(file);

        Ok(MultiSegmentReader {
            buf,
            segs,
            cur,
            off,
        })
    }
}

impl MultiSegmentReader {
    fn is_eof(&mut self) -> io::Result<bool> {
        let is_eof = self.buf.fill_buf()?;
        Ok(is_eof.is_empty())
    }

    fn read_to_buffer(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_read = self.buf.read(buf)?;
        self.off += bytes_read;

        // If we read less than the buffer size, we've reached the end of the current segment.
        if self.off % BLOCK_SIZE != 0 {
            // Fill the rest of the buffer with zeros.
            let i = self.fill_with_zeros(buf, bytes_read);
            self.off += i;
            return Ok(bytes_read + i);
        }

        Ok(bytes_read)
    }

    fn fill_with_zeros(&mut self, buf: &mut [u8], bytes_read: usize) -> usize {
        let mut i = 0;
        while bytes_read + i < buf.len() && (self.off + i) % BLOCK_SIZE != 0 {
            buf[bytes_read + i] = 0;
            i += 1;
        }
        i
    }

    fn load_next_segment(&mut self) -> io::Result<()> {
        if self.cur + 1 >= self.segs.len() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        self.cur += 1;
        self.off = 0;

        let next_file = File::open(&self.segs[self.cur].file_path)?;
        let header_offset = self.segs[self.cur].file_header_offset;
        let mut next_buf_reader = BufReader::new(next_file);
        next_buf_reader.seek(SeekFrom::Start(header_offset))?;

        self.buf = next_buf_reader;

        Ok(())
    }
}

impl Read for MultiSegmentReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.cur >= self.segs.len() {
            return Ok(0);
        }

        if !self.is_eof()? {
            return self.read_to_buffer(buf);
        } else {
            self.load_next_segment()?;
            return self.read_to_buffer(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::wal::segment::Segment;
    use crate::storage::{Options, RECORD_HEADER_SIZE};
    use tempdir::TempDir;

    // Create a mock file that implements Read and Seek traits for testing
    struct MockFile {
        data: Vec<u8>,
        position: usize,
    }

    impl Read for MockFile {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let bytes_read = buf.len().min(self.data.len() - self.position);
            buf[..bytes_read]
                .copy_from_slice(&self.data[self.position..self.position + bytes_read]);
            self.position += bytes_read;
            Ok(bytes_read)
        }
    }

    impl Seek for MockFile {
        fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
            Ok(0)
        }
    }

    #[test]
    fn test_single_segment() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create a sample segment file and populate it with data
        let opts = Options::default();
        let mut segment = Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");

        // Test appending a non-empty buffer
        let r = segment.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Test appending another buffer
        let r = segment.append(&[4, 5, 6, 7]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Create a Vec of segments containing our sample segment
        let mut segments: Vec<SegmentRef> = Vec::new();
        segments.push(SegmentRef {
            file_path: segment.file_path.clone(),
            file_header_offset: segment.file_header_offset,
        });

        // Create a MultiSegmentReader for testing
        let mut buf_reader = MultiSegmentReader::new(segments).expect("should create");

        // Read first record from the MultiSegmentReader
        let mut bs = [0u8; 12];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, 12);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[RECORD_HEADER_SIZE..]);

        // Read second record from the MultiSegmentReader
        let mut bs = [0u8; 12];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, 12);
        assert_eq!(&[4, 5, 6, 7].to_vec(), &bs[RECORD_HEADER_SIZE..]);

        let mut bs = [0u8; 12];
        buf_reader.read(&mut bs).expect_err("should not read");

        // Cleanup: Drop the temp directory, which deletes its contents
        assert!(segment.close().is_ok());
        drop(temp_dir);
    }

    #[test]
    fn test_multi_segment() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create a sample segment file and populate it with data
        let opts = Options::default();
        let mut segment1 =
            Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");
        let mut segment2 =
            Segment::open(&temp_dir.path(), 1, &opts).expect("should create segment");

        // Test appending a non-empty buffer
        let r = segment1.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Test appending another buffer
        let r = segment2.append(&[4, 5, 6, 7]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Create a Vec of segments containing our sample segment
        let mut segments: Vec<SegmentRef> = Vec::new();
        segments.push(SegmentRef {
            file_path: segment1.file_path.clone(),
            file_header_offset: segment1.file_header_offset,
        });
        segments.push(SegmentRef {
            file_path: segment2.file_path.clone(),
            file_header_offset: segment2.file_header_offset,
        });

        // Create a MultiSegmentReader for testing
        let mut buf_reader = MultiSegmentReader::new(segments).expect("should create");

        // Read first record from the MultiSegmentReader
        let mut bs = [0u8; 12];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, 12);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[RECORD_HEADER_SIZE..]);

        // Read second record from the MultiSegmentReader
        let mut bs = [0u8; 12];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, 12);
        assert_eq!(&[4, 5, 6, 7].to_vec(), &bs[RECORD_HEADER_SIZE..]);

        let mut bs = [0u8; 12];
        buf_reader.read(&mut bs).expect_err("should not read");

        // Cleanup: Drop the temp directory, which deletes its contents
        assert!(segment1.close().is_ok());
        assert!(segment2.close().is_ok());
        drop(temp_dir);
    }

    #[test]
    fn test_partial_block() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create a sample segment file and populate it with data
        let opts = Options::default();
        let mut segment = Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");

        // Test appending a non-empty buffer
        let r = segment.append(&[1, 2, 3, 4]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Create a Vec of segments containing our sample segment
        let mut segments: Vec<SegmentRef> = Vec::new();
        segments.push(SegmentRef {
            file_path: segment.file_path.clone(),
            file_header_offset: segment.file_header_offset,
        });

        // Create a MultiSegmentReader for testing
        let mut buf_reader = MultiSegmentReader::new(segments).expect("should create");

        // Read data from the MultiSegmentReader
        let mut bs = [0u8; 50];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, 50);
        assert_eq!(&[1, 2, 3, 4].to_vec(), &bs[RECORD_HEADER_SIZE..12]);
        assert_eq!(buf_reader.off, 50);

        let mut read_buffer = [0u8; 50];
        buf_reader
            .read(&mut read_buffer)
            .expect_err("should not read");
        assert_eq!(buf_reader.off, 50);

        // sync the segment, and read again
        assert!(segment.sync().is_ok());
        let mut read_buffer = [0u8; 50];
        let bytes_read = buf_reader.read(&mut read_buffer).expect("should read");
        assert_eq!(bytes_read, 50);
        assert_eq!(buf_reader.off, 100);

        // Cleanup: Drop the temp directory, which deletes its contents
        assert!(segment.close().is_ok());
        drop(temp_dir);
    }

    #[test]
    fn test_full_synced_block() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create a sample segment file and populate it with data
        let opts = Options::default();
        let mut segment = Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");

        // Test appending a non-empty buffer
        let r = segment.append(&[1, 2, 3, 4]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        assert!(segment.sync().is_ok());

        // Create a Vec of segments containing our sample segment
        let mut segments: Vec<SegmentRef> = Vec::new();
        segments.push(SegmentRef {
            file_path: segment.file_path.clone(),
            file_header_offset: segment.file_header_offset,
        });

        // Create a MultiSegmentReader for testing
        let mut buf_reader = MultiSegmentReader::new(segments).expect("should create");

        // Read data from the MultiSegmentReader
        let mut bs = [0u8; 50];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, 50);
        assert_eq!(&[1, 2, 3, 4].to_vec(), &bs[RECORD_HEADER_SIZE..12]);
        assert_eq!(buf_reader.off, 50);

        let mut read_buffer = [0u8; 50];
        let bytes_read = buf_reader.read(&mut read_buffer).expect("should read");
        assert_eq!(bytes_read, 50);
        assert_eq!(buf_reader.off, 100);
        assert!(read_buffer.iter().all(|&byte| byte == 0));

        // Cleanup: Drop the temp directory, which deletes its contents
        assert!(segment.close().is_ok());
        drop(temp_dir);
    }

    #[test]
    fn test_multi_segment_with_sync() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create a sample segment file and populate it with data
        let opts = Options::default();
        let mut segment1 =
            Segment::open(&temp_dir.path(), 0, &opts).expect("should create segment");
        let mut segment2 =
            Segment::open(&temp_dir.path(), 1, &opts).expect("should create segment");

        // Test appending a non-empty buffer
        let r = segment1.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);
        assert!(segment1.sync().is_ok());

        // Test appending another buffer
        let r = segment2.append(&[4, 5, 6, 7]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);
        assert!(segment2.sync().is_ok());

        // Create a Vec of segments containing our sample segment
        let mut segments: Vec<SegmentRef> = Vec::new();
        segments.push(SegmentRef {
            file_path: segment1.file_path.clone(),
            file_header_offset: segment1.file_header_offset,
        });
        segments.push(SegmentRef {
            file_path: segment2.file_path.clone(),
            file_header_offset: segment2.file_header_offset,
        });

        // Create a MultiSegmentReader for testing
        let mut buf_reader = MultiSegmentReader::new(segments).expect("should create");

        // Read first record from the MultiSegmentReader
        let mut bs = [0u8; BLOCK_SIZE];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, BLOCK_SIZE);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[RECORD_HEADER_SIZE..12]);

        // Read second record from the MultiSegmentReader
        let mut bs = [0u8; BLOCK_SIZE];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, BLOCK_SIZE);
        assert_eq!(&[4, 5, 6, 7].to_vec(), &bs[RECORD_HEADER_SIZE..12]);

        let mut bs = [0u8; 12];
        buf_reader.read(&mut bs).expect_err("should not read");

        // Cleanup: Drop the temp directory, which deletes its contents
        assert!(segment1.close().is_ok());
        assert!(segment2.close().is_ok());
        drop(temp_dir);
    }
}
