use std::io::{self, Read};
use std::vec::Vec;

use crate::storage::log::{
    calculate_crc32, validate_record_type, CorruptionError, Error, IOError, MultiSegmentReader,
    RecordType, Result, BLOCK_SIZE, WAL_RECORD_HEADER_SIZE,
};

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
    err: Option<Error>,
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

    fn read_first_header_byte<R: Read>(rdr: &mut R, buf: &mut [u8]) -> Result<u8> {
        if rdr.read_exact(&mut buf[0..1]).is_err() {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::Other,
                "error reading first header byte",
            )));
        }
        Ok(buf[0])
    }

    fn read_remaining_header<R: Read>(rdr: &mut R, buf: &mut [u8]) -> Result<(u16, u32)> {
        if rdr.read_exact(&mut buf[1..WAL_RECORD_HEADER_SIZE]).is_err() {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::Other,
                "error reading remaining header bytes",
            )));
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
    ) -> Result<(usize, usize)> {
        // Validate the record type.
        validate_record_type(rec_type, current_index)?;

        let record_start = WAL_RECORD_HEADER_SIZE;
        let record_end = record_start + length as usize;
        if rdr.read_exact(&mut buf[record_start..record_end]).is_err() {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::Other,
                "error reading record data",
            )));
        }

        // Validate the checksum.
        let calculated_crc = calculate_crc32(&buf[0..1], &buf[record_start..record_end]);
        if calculated_crc != crc {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::Other,
                "unexpected checksum",
            )));
        }

        Ok((record_start, record_end))
    }

    pub(crate) fn read(&mut self) -> Result<(&[u8], u64)> {
        if let Some(err) = &self.err {
            return Err(err.clone());
        }

        match self.next() {
            Ok(_) => (),
            Err(e) => {
                let (segment_id, offset) =
                    (self.rdr.current_segment_id(), self.rdr.current_offset());
                let err = Error::Corruption(CorruptionError::new(
                    io::ErrorKind::Other,
                    e.to_string().as_str(),
                    segment_id,
                    offset as u64,
                ));
                self.err = Some(err.clone());
                return Err(err);
            }
        }
        Ok((&self.rec, self.rdr.current_offset() as u64))
    }

    fn next(&mut self) -> Result<()> {
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
                    return Err(Error::IO(IOError::new(
                        io::ErrorKind::Other,
                        "error reading remaining zeros",
                    )));
                }
                self.total_read += remaining;

                if !zeros.iter().all(|&c| c == 0) {
                    return Err(Error::IO(IOError::new(
                        io::ErrorKind::Other,
                        "non-zero byte in current block",
                    )));
                }
                continue;
            }

            // Read the rest of the header.
            let (length, crc) = Self::read_remaining_header(&mut self.rdr, &mut self.buf)?;
            self.total_read += WAL_RECORD_HEADER_SIZE - 1;

            // Read the record data.
            let (record_start, record_end) = Self::read_and_validate_record(
                &mut self.rdr,
                &mut self.buf,
                length,
                crc,
                &self.cur_rec_type,
                i,
            )?;
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
    use std::io::BufReader;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::vec::Vec;

    use crate::storage::log::wal::log::Wal;
    use crate::storage::log::{
        read_file_header, Options, Segment, SegmentRef, WAL_RECORD_HEADER_SIZE,
    };
    use tempdir::TempDir;

    // BufferReader does not return EOF when the underlying reader returns 0 bytes read.
    #[test]
    fn bufreader_eof_and_error() {
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

    fn create_test_segment(
        temp_dir: &TempDir,
        id: u64,
        data: &[u8],
    ) -> Segment<WAL_RECORD_HEADER_SIZE> {
        let opts = Options::default().with_wal();
        let mut segment = Segment::open(temp_dir.path(), id, &opts).expect("should create segment");
        let r = segment.append(data);
        assert!(r.is_ok());
        assert_eq!(data.len(), r.unwrap().1);
        segment
    }

    #[test]
    fn reader() {
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
        assert_eq!(reader.total_read, BLOCK_SIZE + 9);

        reader.next().expect("should read");
        assert_eq!(reader.rec, vec![7, 8, 9]);
        assert_eq!(reader.total_read, BLOCK_SIZE * 2 + 10);
    }

    fn create_test_segment_with_data(
        temp_dir: &TempDir,
        id: u64,
    ) -> Segment<WAL_RECORD_HEADER_SIZE> {
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
    fn reader_with_single_segment_and_multiple_records() {
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
    fn reader_with_multiple_segments_and_multiple_records() {
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
    fn reader_with_wal() {
        // Create aol options and open a aol file
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        let opts = Options::default().with_max_file_size(4096 * 10).with_wal();
        let mut a = Wal::open(temp_dir.path(), opts).expect("should create aol");

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
    fn wal_repair() {
        // Create a temporary directory to hold the segment files
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create sample segment file and populate it with data
        let mut segment = create_test_segment(&temp_dir, 4, &[1, 2, 3, 4]);
        segment.append(&[5, 6, 7, 8]).expect("should append");

        // Close the segment file
        assert!(segment.close().is_ok());

        // Open the segment file for reading and writing
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&segment.file_path)
            .expect("should open");

        // Read the file header and move the cursor to the first record
        read_file_header(&mut file).expect("should read");

        // Corrupt the checksum of the second record
        let offset_to_edit = 195;
        let new_byte_value = 0x55;
        file.seek(SeekFrom::Start(offset_to_edit as u64))
            .expect("should seek");
        file.write_all(&[new_byte_value]).expect("should write");

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
            match err {
                Error::Corruption(corruption_error) => {
                    // assert_eq!(corruption_error.message, "unexpected checksum");
                    assert_eq!(corruption_error.segment_id, 4);
                    assert_eq!(corruption_error.offset, 22);
                    corrupted_segment_id = corruption_error.segment_id;
                    corrupted_offset_marker = corruption_error.offset as u64;
                }
                _ => panic!("Expected a CorruptionError, but got a different error"),
            }
        }

        // Repair the corrupted segment
        let opts = Options::default().with_wal();
        let mut a = Wal::open(temp_dir.path(), opts).expect("should create wal");
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
            assert_eq!(reader.total_read, BLOCK_SIZE + 14);
        }
    }

    // TODO: add more tests for wal repair
}
