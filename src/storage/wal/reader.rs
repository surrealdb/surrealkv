use std::fs::File;
use std::io::BufReader;
use std::io::{self, BufRead, Read, Seek, SeekFrom};
use std::vec::Vec;

use crate::storage::{
    calculate_crc32, validate_record, RecordType, SegmentRef, BLOCK_SIZE, RECORD_HEADER_SIZE,
};

pub struct MultiSegmentReader {
    buf: BufReader<File>,
    segs: Vec<SegmentRef>,
    cur: usize, // Index of current segment in segs.
    off: usize, // Offset in current segment.
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

        // TODO: could create a problem when reading a partial block spread over multiple segments
        if !self.is_eof()? {
            return self.read_to_buffer(buf);
        } else {
            self.load_next_segment()?;
            return self.read_to_buffer(buf);
        }
    }
}

pub struct Reader {
    rdr: MultiSegmentReader,
    rec: Vec<u8>,
    buf: [u8; BLOCK_SIZE],
    total_read: usize,
    cur_rec_type: RecordType,
}

impl Reader {
    fn new(rdr: MultiSegmentReader) -> Self {
        Reader {
            rdr,
            rec: Vec::new(),
            buf: [0u8; BLOCK_SIZE],
            total_read: 0,
            cur_rec_type: RecordType::Empty,
        }
    }

    // TODO: return segment id and offset in error
    fn next(&mut self) -> Result<(), io::Error> {
        self.rec.clear();
        let mut i = 0;

        loop {
            // Read first byte of header.
            if self.rdr.read_exact(&mut self.buf[0..1]).is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "error reading first header byte",
                ));
            }
            self.total_read += 1;
            self.cur_rec_type = RecordType::from_u8(self.buf[0])?;

            // If the first byte is 0, it's a padded page.
            // Read the rest of the page of zeros and continue.
            if self.cur_rec_type == RecordType::Empty {
                let zeros = &mut self.buf[1..];
                let k = (BLOCK_SIZE - (self.total_read % BLOCK_SIZE)) as usize;
                if k == BLOCK_SIZE as usize {
                    continue; // Initial 0 byte was last page byte.
                }
                if self.rdr.read_exact(&mut zeros[..k]).is_err() {
                    return Err(io::Error::new(io::ErrorKind::Other, "read remaining zeros"));
                }
                self.total_read += k;

                if !zeros[..k].iter().all(|&c| c == 0) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "unexpected non-zero byte in padded block",
                    ));
                }
                continue;
            }

            // Read the rest of the header.
            if self
                .rdr
                .read_exact(&mut self.buf[1..RECORD_HEADER_SIZE])
                .is_err()
            {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "error reading remaining header bytes",
                ));
            }
            self.total_read += (RECORD_HEADER_SIZE - 1);

            let length = u16::from_be_bytes([self.buf[2], self.buf[3]]);
            let crc = u32::from_be_bytes([self.buf[4], self.buf[5], self.buf[6], self.buf[7]]);

            // Read the record data.
            if self
                .rdr
                .read_exact(
                    &mut self.buf[RECORD_HEADER_SIZE..(RECORD_HEADER_SIZE + length as usize)],
                )
                .is_err()
            {
                return Err(io::Error::new(io::ErrorKind::Other, "error reading record data"));
            }
            self.total_read += length as usize;

            // Validate the checksum.
            let calculated_crc = calculate_crc32(
                &self.buf[RECORD_HEADER_SIZE..(RECORD_HEADER_SIZE + length as usize)],
            );
            if calculated_crc != crc {
                return Err(io::Error::new(io::ErrorKind::Other, "unexpected checksum"));
            }

            // Copy the record data to the output buffer.
            self.rec.extend_from_slice(
                &self.buf[RECORD_HEADER_SIZE..(RECORD_HEADER_SIZE + length as usize)],
            );

            // Validate the record type.
            validate_record(&self.cur_rec_type, i)?;

            if self.cur_rec_type == RecordType::Last || self.cur_rec_type == RecordType::Full {
                break;
            }

            i += 1;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

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

    // BufferReader does not return EOF when the underlying reader returns 0 bytes read.
    #[test]
    fn test_bufreader_eof_and_error() {
        // Create a temporary directory to hold the file
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create a sample file and populate it with data
        let file_path = temp_dir.path().join("test.txt");
        let mut file = File::create(&file_path).expect("should create file");
        file.write_all(b"Hello, World!").expect("should write data");
        drop(file);

        // Open the file for reading using BufReader
        let file = File::open(&file_path).expect("should open file");
        let mut buf_reader = BufReader::new(file);

        // Read into a buffer
        let mut read_buffer = [0u8; 5];
        let bytes_read = buf_reader.read(&mut read_buffer).expect("should read");

        // Verify the data read
        assert_eq!(&read_buffer[..bytes_read], b"Hello");

        // Try reading more bytes than available
        let mut read_buffer = [0u8; 10];
        let bytes_read = buf_reader.read(&mut read_buffer).expect("should read");
        assert_eq!(bytes_read, 8); // Only "World!" left to read
        assert_eq!(&read_buffer[..bytes_read], b", World!");

        // Try reading more bytes again than available
        let mut read_buffer = [0u8; 1000];
        let bytes_read = buf_reader.read(&mut read_buffer).expect("should read");
        assert_eq!(bytes_read, 0); // Only "World!" left to read
    }

    fn create_test_segment(temp_dir: &TempDir, id: u64, data: &[u8]) -> Segment {
        let opts = Options::default();
        let mut segment =
            Segment::open(&temp_dir.path(), id, &opts).expect("should create segment");
        let r = segment.append(data);
        assert!(r.is_ok());
        assert_eq!(data.len(), r.unwrap().1);
        segment
    }

    fn create_test_segment_ref(segment: &Segment) -> SegmentRef {
        SegmentRef {
            file_path: segment.file_path.clone(),
            file_header_offset: segment.file_header_offset,
            id: segment.id,
        }
    }

    #[test]
    fn test_single_segment() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create a sample segment file and populate it with data
        let mut segment = create_test_segment(&temp_dir, 0, &[0, 1, 2, 3]);

        // Test appending another buffer
        let r = segment.append(&[4, 5, 6, 7]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Create a Vec of segments containing our sample segment
        let segments: Vec<SegmentRef> = vec![create_test_segment_ref(&segment)];

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
        let segments: Vec<SegmentRef> = vec![
            create_test_segment_ref(&segment1),
            create_test_segment_ref(&segment2),
        ];

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
        let segments: Vec<SegmentRef> = vec![create_test_segment_ref(&segment)];

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
        let segments: Vec<SegmentRef> = vec![create_test_segment_ref(&segment)];
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
        let segments: Vec<SegmentRef> = vec![
            create_test_segment_ref(&segment1),
            create_test_segment_ref(&segment2),
        ];

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

    #[test]
    fn test_segment_ref() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create sample segment files and populate it with data
        let mut segment1 = create_test_segment(&temp_dir, 4, &[1, 2, 3, 4]);
        let mut segment2 = create_test_segment(&temp_dir, 6, &[5, 6]);
        let mut segment3 = create_test_segment(&temp_dir, 8, &[7, 8, 9]);
        assert!(segment1.close().is_ok());
        assert!(segment2.close().is_ok());
        assert!(segment3.close().is_ok());

        let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
            .expect("should read segments");
        assert!(sr.len() == 3);
        assert!(sr[0].id == 4);
        assert!(sr[1].id == 6);
        assert!(sr[2].id == 8);

        drop(temp_dir);
    }

    #[test]
    fn test_reader() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create sample segment files and populate it with data
        let mut segment1 = create_test_segment(&temp_dir, 4, &[1, 2, 3, 4]);
        let mut segment2 = create_test_segment(&temp_dir, 6, &[5, 6]);
        let mut segment3 = create_test_segment(&temp_dir, 8, &[7, 8, 9]);
        assert!(segment1.close().is_ok());
        assert!(segment2.close().is_ok());
        assert!(segment3.close().is_ok());

        let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
            .expect("should read segments");

        let mut reader = Reader::new(MultiSegmentReader::new(sr).expect("should create"));
        reader.next().expect("should read");
        assert_eq!(reader.rec, vec![1, 2, 3, 4]);
        assert_eq!(reader.total_read, 12);

        reader.next().expect("should read");
        assert_eq!(reader.rec, vec![5, 6]);
        assert_eq!(reader.total_read, 4106);

        reader.next().expect("should read");
        assert_eq!(reader.rec, vec![7, 8, 9]);
        assert_eq!(reader.total_read, 8203);
    }
}
