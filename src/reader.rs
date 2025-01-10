use bytes::BytesMut;
use std::io::Read;

use crate::entry::{Record, MAX_KV_METADATA_SIZE};
use crate::error::{Error, Result};
use crate::log::{CorruptionError, Error as LogError, Error::Corruption, MultiSegmentReader};
use crate::meta::Metadata;

/// `Reader` is a generic reader for reading data from an Aol. It is used
/// by the `RecordReader` to read data from the Aol source.
pub struct Reader {
    rdr: MultiSegmentReader,
    err: Option<Error>,
}

impl Reader {
    /// Creates a new `Reader` with the given `rdr`, `off`, and `size`.
    pub fn new_from(rdr: MultiSegmentReader) -> Self {
        Reader { rdr, err: None }
    }

    /// Returns the current offset of the `Reader`.
    fn offset(&self) -> u64 {
        self.rdr.current_offset() as u64
    }

    fn current_segment_id(&self) -> u64 {
        self.rdr.current_segment_id()
    }

    fn current_offset(&self) -> u64 {
        self.rdr.current_offset() as u64
    }

    /// Reads data into the provided buffer.
    pub(crate) fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let Some(err) = &self.err {
            return Err(err.clone());
        }

        if let Err(e) = self.rdr.read_exact(buf) {
            let (segment_id, offset) = (self.rdr.current_segment_id(), self.rdr.current_offset());

            let err = match e.kind() {
                std::io::ErrorKind::UnexpectedEof => Error::LogError(LogError::Eof),
                _ => {
                    // Default action for other errors
                    Error::LogError(Corruption(CorruptionError::new(
                        std::io::ErrorKind::Other,
                        e.to_string().as_str(),
                        segment_id,
                        offset as u64,
                    )))
                }
            };

            self.err = Some(err.clone());

            return Err(err);
        }

        Ok(buf.len())
    }

    /// Reads a single byte from the data source.
    #[allow(dead_code)]
    pub(crate) fn read_byte(&mut self) -> Result<u8> {
        let mut b = [0; 1];
        self.read(&mut b)?;
        Ok(b[0])
    }

    /// Reads a specified number of bytes from the data source.
    ///
    /// # Arguments
    ///
    /// * `len: usize` - The number of bytes to read.
    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0; len];
        self.read(&mut buffer)?;
        Ok(buffer)
    }

    /// Reads a 64-bit unsigned integer from the data source.
    pub(crate) fn read_uint64(&mut self) -> Result<u64> {
        let mut b = [0; 8];
        self.read(&mut b)?;
        Ok(u64::from_be_bytes(b))
    }

    /// Reads a 32-bit unsigned integer from the data source.
    pub(crate) fn read_uint32(&mut self) -> Result<u32> {
        let mut b = [0; 4];
        self.read(&mut b)?;
        Ok(u32::from_be_bytes(b))
    }

    /// Reads a 16-bit unsigned integer from the data source.
    pub(crate) fn read_uint16(&mut self) -> Result<u16> {
        let mut b = [0; 2];
        self.read(&mut b)?;
        Ok(u16::from_be_bytes(b))
    }
}

/// `RecordReader` is a public struct within the crate that is used for reading transaction records.
///
/// # Fields
/// * `r: Reader` - The `Reader` instance used to read data.
pub struct RecordReader {
    r: Reader,
    err: Option<Error>,
    rec: Vec<u8>,
}

impl RecordReader {
    /// Creates a new `RecordReader` with the given `Reader`.
    ///
    /// # Arguments
    ///
    /// * `r: Reader` - The `Reader` instance to use for reading data.
    pub fn new(r: Reader) -> Self {
        RecordReader {
            r,
            err: None,
            rec: Vec::new(),
        }
    }

    /// Reads a transaction entry.
    fn read_entry_into(&mut self, rec: &mut Record) -> Result<(u64, u64)> {
        // Reading the first record can return an EOF, which does not indicate corruption.
        let entry_crc = self.r.read_uint32()?;

        // Store initial segment and offset
        // Note that we do not store records spanning two segments,
        // so the initial segment and offset are sufficient to identify the record.
        let initial_segment = self.r.current_segment_id();
        // Note: If any record is corrupted at a later stage, this offset
        // will not reflect the actual offset where the corruption occurred, rather
        // the offset where the corrupted record started.
        let initial_offset = self.r.current_offset();

        // Helper closure to create errors with initial position
        let corrupt_error = |msg: &str, e: Error| {
            Error::LogError(Corruption(CorruptionError::new(
                std::io::ErrorKind::Other,
                Error::CorruptedTransactionRecord(format!("{}: {}", msg, e))
                    .to_string()
                    .as_str(),
                initial_segment,
                initial_offset,
            )))
        };

        let segment_id = self.r.current_segment_id();

        let version = self
            .r
            .read_uint16()
            .map_err(|e| corrupt_error("Failed to read version", e))?;
        let id = self
            .r
            .read_uint64()
            .map_err(|e| corrupt_error("Failed to read id", e))?;

        // Either the header is corrupted or we have reached the end of the file
        // and encountered the padded zeros towards the end of the file.
        if id == 0 {
            return Err(corrupt_error(
                "Failed to read tx id",
                Error::InvalidTransactionRecordId,
            ));
        }

        let ts = self
            .r
            .read_uint64()
            .map_err(|e| corrupt_error("Failed to read timestamp", e))?;

        let md_len = self
            .r
            .read_uint16()
            .map_err(|e| corrupt_error("Failed to read metadata length", e))?
            as usize;
        if md_len > MAX_KV_METADATA_SIZE {
            return Err(corrupt_error(
                "Metadata length exceeds maximum",
                Error::MaxKVMetadataLengthExceeded,
            ));
        }

        let kvmd = if md_len > 0 {
            let md_bs = self
                .r
                .read_bytes(md_len)
                .map_err(|e| corrupt_error("Failed to read metadata bytes", e))?;
            let metadata = Metadata::from_bytes(&md_bs)
                .map_err(|e| corrupt_error("Failed to parse metadata", e))?;
            Some(metadata)
        } else {
            None
        };

        let k_len =
            self.r
                .read_uint32()
                .map_err(|e| corrupt_error("Failed to read key length", e))? as usize;
        let k = self
            .r
            .read_bytes(k_len)
            .map_err(|e| corrupt_error("Failed to read key bytes", e))?;
        let v_len =
            self.r
                .read_uint32()
                .map_err(|e| corrupt_error("Failed to read value length", e))? as usize;
        let offset = self.r.offset();
        let v = self
            .r
            .read_bytes(v_len)
            .map_err(|e| corrupt_error("Failed to read value bytes", e))?;

        rec.id = id;
        rec.ts = ts;
        rec.version = version;
        rec.metadata = kvmd;
        rec.key = k.into();
        rec.value = v.into();
        rec.value_len = v_len as u32;
        rec.key_len = k_len as u32;

        let actual_crc = rec.calculate_crc32();
        if entry_crc != actual_crc {
            return Err(corrupt_error(
                "CRC mismatch",
                Error::ChecksumMismatch(entry_crc, actual_crc),
            ));
        }

        rec.crc32 = entry_crc;

        Ok((segment_id, offset))
    }

    /// Reads a transaction record into the provided `Record`.
    pub fn read_into(&mut self, entry: &mut Record) -> Result<(u64, u64)> {
        self.read_entry_into(entry)
    }

    pub(crate) fn read(&mut self) -> Result<(&[u8], u64)> {
        if let Some(err) = &self.err {
            return Err(err.clone());
        }

        self.rec.clear();

        let mut tx = Record::new();
        self.read_into(&mut tx)?;

        let mut rec = BytesMut::new();
        tx.encode(&mut rec)?;
        let rec = rec.freeze();

        self.rec.extend(rec);

        Ok((&self.rec, self.r.current_offset()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{Aol, Options, SegmentRef};
    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[test]
    fn reader() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options {
            max_file_size: 4,
            ..Options::default()
        };
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Test initial offset
        let sz = (a.active_segment_id, a.active_segment.offset());
        assert_eq!(0, sz.0);
        assert_eq!(0, sz.1);

        // Test appending a non-empty buffer
        let r = a.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().2);

        // Test appending another buffer
        let r = a.append(&[4, 5, 6, 7]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().2);

        a.close().expect("should close aol");

        let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
            .expect("should read segments");
        let sr = MultiSegmentReader::new(sr).expect("should create segment reader");

        let mut r = Reader::new_from(sr);

        let mut bs = vec![0; 4];
        let n = r.read(&mut bs).expect("should read");
        assert_eq!(4, n);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[..]);

        let mut bs = vec![0; 4];
        let n = r.read(&mut bs).expect("should read");
        assert_eq!(4, n);
        assert_eq!(&[4, 5, 6, 7].to_vec(), &bs[..]);

        let mut bs = vec![0; 4];
        assert!(match r.read(&mut bs) {
            Err(err) => {
                matches!(err, Error::LogError(LogError::Eof))
            }
            _ => false,
        });
    }

    #[test]
    fn append_and_read() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        const REC_SIZE: usize = 20;
        let num_items = 100;
        // Create aol options and open a aol file
        let opts = Options {
            max_file_size: 40,
            ..Options::default()
        };

        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Append 10 records
        for i in 0..num_items {
            let r = a.append(&[i; REC_SIZE]); // Each record is a 4-byte array filled with `i`
            assert!(r.is_ok());
            assert_eq!(REC_SIZE, r.unwrap().2);
        }

        a.close().expect("should close aol");

        let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
            .expect("should read segments");
        let sr = MultiSegmentReader::new(sr).expect("should create segment reader");

        let mut r = Reader::new_from(sr);

        // Read and verify the 10 records
        for i in 0..num_items {
            let mut bs = vec![0; REC_SIZE];
            let n = r.read(&mut bs).expect("should read");
            assert_eq!(REC_SIZE, n);
            assert_eq!(&[i; REC_SIZE].to_vec(), &bs[..]);
        }

        // Verify that we've reached the end of the file
        let mut bs = vec![0; REC_SIZE];
        assert!(match r.read(&mut bs) {
            Err(err) => {
                matches!(err, Error::LogError(LogError::Eof))
            }
            _ => false,
        });
    }
}
