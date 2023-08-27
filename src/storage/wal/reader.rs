use std::fs::File;
use std::io::BufReader;
use std::io::{self, BufRead, Read, Seek, SeekFrom};
use std::vec::Vec;

use crate::storage::wal::CorruptionError;
use crate::storage::{
    calculate_crc32, validate_record_type, RecordType, SegmentRef, BLOCK_SIZE,
    WAL_RECORD_HEADER_SIZE,
};

pub struct MultiSegmentReader {
    buf: BufReader<File>,  // Buffer for reading from the current segment.
    segments: Vec<SegmentRef>, // List of segments to read from.
    cur: usize,            // Index of current segment in segments.
    off: usize,            // Offset in current segment.
}

impl MultiSegmentReader {
    pub(crate) fn new(segments: Vec<SegmentRef>) -> Result<MultiSegmentReader, io::Error> {
        if segments.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Empty segment list",
            ));
        }

        let cur = 0;
        let off = 0;

        // Open the first segment's file for reading
        let mut file = File::open(&segments[cur].file_path)?;
        file.seek(SeekFrom::Start(segments[cur].file_header_offset))?;

        let buf = BufReader::new(file);

        Ok(MultiSegmentReader {
            buf,
            segments,
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
        if self.cur + 1 >= self.segments.len() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        self.cur += 1;
        self.off = 0;

        let next_file = File::open(&self.segments[self.cur].file_path)?;
        let header_offset = self.segments[self.cur].file_header_offset;
        let mut next_buf_reader = BufReader::new(next_file);
        next_buf_reader.seek(SeekFrom::Start(header_offset))?;

        self.buf = next_buf_reader;

        Ok(())
    }

    fn current_segment_id(&self) -> u64 {
        self.segments[self.cur].id
    }

    fn current_offset(&self) -> usize {
        self.off
    }
}

impl Read for MultiSegmentReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.cur >= self.segments.len() {
            return Ok(0);
        }

        // TODO: could create a problem when reading a partial block spread over multiple segments
        if !self.is_eof()? {
            self.read_to_buffer(buf)
        } else {
            self.load_next_segment()?;
            self.read_to_buffer(buf)
        }
    }
}

// Reader reads records from a MultiSegmentReader. The records are returned in the order they were written.
// The current implementation of Reader is inspired by levelDB and prometheus implementation for WAL.
// Since the WAL is append-only, the records are written in frames of BLOCK_SIZE bytes. The first byte of 
// each frame is the record type, followed by a reserved byte, followed by the record length (2 bytes) and 
// the CRC32 checksum (4 bytes). The record data follows the checksum.
//
// No partial writes are allowed. If the segment is not full, and the record can't fit in the remaining space, the
// segment is padded with zeros. This is important for the reader to be able to read the records in BLOCK_SIZE chunks.
pub struct Reader {
    rdr: MultiSegmentReader,
    rec: Vec<u8>,
    buf: [u8; BLOCK_SIZE],
    total_read: usize,
    cur_rec_type: RecordType,
    err: Option<CorruptionError>,
}

impl Reader {
    pub(crate) fn new(rdr: MultiSegmentReader) -> Self {
        Reader {
            rdr,
            rec: Vec::new(),
            buf: [0u8; BLOCK_SIZE],
            total_read: 0,
            cur_rec_type: RecordType::Empty,
            err: None,
        }
    }

    fn read_first_header_byte<R: Read>(rdr: &mut R, buf: &mut [u8]) -> Result<u8, io::Error> {
        if rdr.read_exact(&mut buf[0..1]).is_err() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "error reading first header byte",
            ));
        }
        Ok(buf[0])
    }

    fn read_remaining_header<R: Read>(
        rdr: &mut R,
        buf: &mut [u8],
    ) -> Result<(u16, u32), io::Error> {
        if rdr.read_exact(&mut buf[1..WAL_RECORD_HEADER_SIZE]).is_err() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "error reading remaining header bytes",
            ));
        }

        let length = u16::from_be_bytes([buf[1], buf[2]]);
        let crc = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]);
        Ok((length, crc))
    }

    fn read_and_validate_record<R: Read>(
        rdr: &mut R,
        buf: &mut [u8],
        length: u16,
        crc: u32,
        rec_type: &RecordType,
        current_index: usize,
    ) -> Result<(usize, usize), io::Error> {
        // Validate the record type.
        validate_record_type(rec_type, current_index)?;


        let record_start = WAL_RECORD_HEADER_SIZE;
        let record_end = record_start + length as usize;
        if rdr.read_exact(&mut buf[record_start..record_end]).is_err() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "error reading record data",
            ));
        }

        // Validate the checksum.
        let calculated_crc = calculate_crc32(&buf[0..1], &buf[record_start..record_end]);
        if calculated_crc != crc {
            return Err(io::Error::new(io::ErrorKind::Other, "unexpected checksum"));
        }

        Ok((record_start, record_end))
    }

    pub(crate) fn read(&mut self) -> Result<(&[u8], u64), CorruptionError> {
        if let Some(err) = self.err.as_ref() {
            return Err(err.clone());
        }

        match self.next() {
            Ok(_) => (),
            Err(e) => {
                let (segment_id, offset) =
                    (self.rdr.current_segment_id(), self.rdr.current_offset());
                let err =
                    CorruptionError::new(e.kind(), e.to_string().as_str(), segment_id, offset);
                self.err = Some(err.clone());
                return Err(err);
            }
        }
        Ok((&self.rec, self.rdr.current_offset() as u64))
    }

    fn next(&mut self) -> Result<(), io::Error> {
        self.rec.clear();
        let mut i = 0;

        loop {
            // Read first byte of header to determine record type.
            let first_byte = Self::read_first_header_byte(&mut self.rdr, &mut self.buf[0..1])?;
            self.total_read += 1;
            self.cur_rec_type = RecordType::from_u8(first_byte)?;

            // If the first byte is 0, it's a padded page.
            // Read the rest of the page of zeros and continue.
            if self.cur_rec_type == RecordType::Empty {
                let remaining = BLOCK_SIZE - (self.total_read % BLOCK_SIZE);
                if remaining == BLOCK_SIZE {
                    continue;
                }

                let zeros = &mut self.buf[1..remaining + 1];
                if self.rdr.read_exact(zeros).is_err() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "error reading remaining zeros",
                    ));
                }
                self.total_read += remaining;

                if !zeros.iter().all(|&c| c == 0) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "non-zero byte in current block",
                    ));
                }
                continue;
            }

            // Read the rest of the header.
            let (length, crc) = Self::read_remaining_header(&mut self.rdr, &mut self.buf)?;
            self.total_read += WAL_RECORD_HEADER_SIZE - 1;

            // Read the record data.
            let (record_start, record_end) =
                Self::read_and_validate_record(&mut self.rdr, &mut self.buf, length, crc, &self.cur_rec_type, i)?;
            self.total_read += length as usize;

            // Copy the record data to the output buffer.
            self.rec
                .extend_from_slice(&self.buf[record_start..record_end]);

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
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, Write};
    use std::vec::Vec;

    use crate::storage::wal::wal::WAL;
    use crate::storage::{read_file_header, Options, Segment, WAL_RECORD_HEADER_SIZE};
    use tempdir::TempDir;

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
        let opts = Options::default().with_wal();
        let mut segment = Segment::open(temp_dir.path(), id, &opts).expect("should create segment");
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
        let mut bs = [0u8; 11];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, 11);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        // Read second record from the MultiSegmentReader
        let mut bs = [0u8; 11];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, 11);
        assert_eq!(&[4, 5, 6, 7].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        let mut bs = [0u8; 11];
        buf_reader.read(&mut bs).expect_err("should not read");

        assert!(segment.close().is_ok());
    }

    #[test]
    fn test_multi_segment() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create a sample segment file and populate it with data
        let opts = Options::default().with_wal();
        let mut segment1 = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");
        let mut segment2 = Segment::open(temp_dir.path(), 1, &opts).expect("should create segment");

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
        let mut bs = [0u8; 11];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, 11);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        // Read second record from the MultiSegmentReader
        let mut bs = [0u8; 11];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, 11);
        assert_eq!(&[4, 5, 6, 7].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        let mut bs = [0u8; 11];
        buf_reader.read(&mut bs).expect_err("should not read");

        assert!(segment1.close().is_ok());
        assert!(segment2.close().is_ok());
    }

    #[test]
    fn test_partial_block() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create a sample segment file and populate it with data
        let opts = Options::default().with_wal();
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

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
        assert_eq!(&[1, 2, 3, 4].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..11]);
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

        assert!(segment.close().is_ok());
    }

    #[test]
    fn test_full_synced_block() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create a sample segment file and populate it with data
        let opts = Options::default().with_wal();
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

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
        assert_eq!(&[1, 2, 3, 4].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..11]);
        assert_eq!(buf_reader.off, 50);

        let mut read_buffer = [0u8; 50];
        let bytes_read = buf_reader.read(&mut read_buffer).expect("should read");
        assert_eq!(bytes_read, 50);
        assert_eq!(buf_reader.off, 100);
        assert!(read_buffer.iter().all(|&byte| byte == 0));

        assert!(segment.close().is_ok());
    }

    #[test]
    fn test_multi_segment_with_sync() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create a sample segment file and populate it with data
        let opts = Options::default().with_wal();
        let mut segment1 = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");
        let mut segment2 = Segment::open(temp_dir.path(), 1, &opts).expect("should create segment");

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
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..11]);

        // Read second record from the MultiSegmentReader
        let mut bs = [0u8; BLOCK_SIZE];
        let bytes_read = buf_reader.read(&mut bs).expect("should read");
        assert_eq!(bytes_read, BLOCK_SIZE);
        assert_eq!(&[4, 5, 6, 7].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..11]);

        let mut bs = [0u8; 11];
        buf_reader.read(&mut bs).expect_err("should not read");

        assert!(segment1.close().is_ok());
        assert!(segment2.close().is_ok());
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
        assert_eq!(reader.total_read, 11);

        reader.next().expect("should read");
        assert_eq!(reader.rec, vec![5, 6]);
        assert_eq!(reader.total_read, 4105);

        reader.next().expect("should read");
        assert_eq!(reader.rec, vec![7, 8, 9]);
        assert_eq!(reader.total_read, 8202);
    }

    fn create_test_segment_with_data(temp_dir: &TempDir, id: u64) -> Segment {
        let opts = Options::default().with_wal();
        let mut segment = Segment::open(temp_dir.path(), id, &opts).expect("should create segment");

        let record_size = 4;
        let num_records = 1000;

        for _ in 0..num_records {
            let data: Vec<u8> = (0..record_size).map(|i| (i & 0xFF) as u8).collect();
            let r = segment.append(&data);
            assert!(r.is_ok());
            assert_eq!(data.len(), r.unwrap().1);
        }

        segment
    }

    #[test]
    fn test_reader_with_single_segment_and_multiple_records() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create sample segment files and populate it with data
        let mut segment1 = create_test_segment_with_data(&temp_dir, 4);
        assert!(segment1.close().is_ok());

        let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
            .expect("should read segments");

        let mut reader = Reader::new(MultiSegmentReader::new(sr).expect("should create"));

        let mut i = 0;
        while let Ok((data, _)) = reader.read() {
            assert_eq!(data, vec![0, 1, 2, 3]);
            i += 1;
        }

        assert_eq!(i, 1000);
    }

    #[test]
    fn test_reader_with_multiple_segments_and_multiple_records() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create sample segment files and populate it with data
        let mut segment1 = create_test_segment_with_data(&temp_dir, 4);
        let mut segment2 = create_test_segment_with_data(&temp_dir, 6);
        let mut segment3 = create_test_segment_with_data(&temp_dir, 8);
        assert!(segment1.close().is_ok());
        assert!(segment2.close().is_ok());
        assert!(segment3.close().is_ok());

        let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
            .expect("should read segments");

        let mut reader = Reader::new(MultiSegmentReader::new(sr).expect("should create"));

        let mut i = 0;
        while let Ok((data, _)) = reader.read() {
            assert_eq!(data, vec![0, 1, 2, 3]);
            i += 1;
        }

        assert_eq!(i, 3000);
    }

    #[test]
    fn test_reader_with_wal() {
        // Create aol options and open a aol file
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        let opts = Options::default().with_max_file_size(4096 * 10).with_wal();
        let mut a = WAL::open(temp_dir.path(), &opts).expect("should create aol");

        let record_size = 4;

        // Define the number of records to append
        let num_records = 10000;

        for _ in 0..num_records {
            let data: Vec<u8> = (0..record_size).map(|i| (i & 0xFF) as u8).collect();
            let r = a.append(&data);
            assert!(r.is_ok());
            assert_eq!(data.len() + WAL_RECORD_HEADER_SIZE, r.unwrap().1);
        }

        a.sync().expect("should sync");

        let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
            .expect("should read segments");

        let mut reader = Reader::new(MultiSegmentReader::new(sr).expect("should create"));

        let mut i = 0;
        while let Ok((data, _)) = reader.read() {
            assert_eq!(data, vec![0, 1, 2, 3]);
            i += 1;
        }

        assert_eq!(i, num_records);
    }

    #[test]
    fn test_wal_repair() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create sample segment file and populate it with data
        let mut segment = create_test_segment(&temp_dir, 4, &[1, 2, 3, 4]);
        segment.append(&[5, 6, 7, 8]).expect("should append");

        // Open the segment file for reading and writing
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&segment.file_path)
            .expect("should open");

        // Read the file header and move the cursor to the first record
        read_file_header(&mut file).expect("should read");

        // Corrupt the checksum of the second record
        let offset_to_edit = 162;
        let new_byte_value = 0x55;
        file.seek(SeekFrom::Start(offset_to_edit as u64))
            .expect("should seek");
        file.write_all(&[new_byte_value]).expect("should write");

        // Close the segment file
        assert!(segment.close().is_ok());

        // Read and repair the corrupted segment
        let corrupted_segment_id;
        let corrupted_offset_marker;

        {
            let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
                .expect("should read segments");

            let mut reader = Reader::new(MultiSegmentReader::new(sr).expect("should create"));

            // Read the valid records before corruption
            let rec = reader.read().expect("should read");
            assert_eq!(rec.0, vec![1, 2, 3, 4]);
            assert_eq!(reader.total_read, 11);

            // Simulate a checksum error
            let err = reader.read().expect_err("should get checksum error");
            assert_eq!(err.message, "unexpected checksum");
            assert_eq!(err.segment_id, 4);
            assert_eq!(err.offset, 22);

            corrupted_segment_id = err.segment_id;
            corrupted_offset_marker = err.offset as u64;
        }

        // Repair the corrupted segment
        let opts = Options::default().with_wal();
        let mut a = WAL::open(temp_dir.path(), &opts).expect("should create wal");
        a.repair(corrupted_segment_id, corrupted_offset_marker)
            .expect("should repair");

        // Verify the repaired segment
        {
            let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
                .expect("should read segments");

            let mut reader = Reader::new(MultiSegmentReader::new(sr).expect("should create"));

            // Read the valid records after repair
            let rec = reader.read().expect("should read");
            assert_eq!(rec.0, vec![1, 2, 3, 4]);
            assert_eq!(reader.total_read, 11);

            // Ensure no further records can be read
            reader.read().expect_err("should not read");
        }

        // Append new data to the repaired segment
        let r = a.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());
        assert_eq!(14, r.unwrap().1);
        assert!(a.close().is_ok());

        // Verify the appended data
        {
            let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
                .expect("should read segments");

            let mut reader = Reader::new(MultiSegmentReader::new(sr).expect("should create"));

            // Read the valid records after append
            let rec = reader.read().expect("should read");
            assert_eq!(rec.0, vec![1, 2, 3, 4]);
            assert_eq!(reader.total_read, 11);

            let rec = reader.read().expect("should read");
            assert_eq!(rec.0, vec![4, 5, 6, 7, 8, 9, 10]);
            assert_eq!(reader.total_read, 4110);
        }
    }

    // TODO: add more tests for wal repair
}
