use std::io::Read;

use bytes::{BufMut, Bytes, BytesMut};

use hashbrown::HashMap;

use crate::storage::{
    kv::{
        entry::{TxEntry, TxRecord, MAX_KV_METADATA_SIZE, MAX_TX_METADATA_SIZE},
        error::{Error, Result},
        meta::Metadata,
        util::calculate_crc32,
    },
    log::{CorruptionError, Error as LogError, Error::Corruption, MultiSegmentReader},
};

/// `Reader` is a generic reader for reading data from an Aol. It is used
/// by the `TxReader` to read data from the Aol source.
pub(crate) struct Reader {
    rdr: MultiSegmentReader,
    buffer: Vec<u8>,
    read: usize,
    start: usize,
    file_size: u64,
    err: Option<Error>,
}

impl Reader {
    /// Creates a new `Reader` with the given `rdr`, `off`, and `size`.
    pub(crate) fn new_from(rdr: MultiSegmentReader, file_size: u64, size: usize) -> Self {
        Reader {
            rdr,
            buffer: vec![0; size],
            read: 0,
            start: 0,
            file_size,
            err: None,
        }
    }

    /// Returns the current offset of the `Reader`.
    fn offset(&self) -> u64 {
        self.rdr.current_segment_id() * self.file_size + self.rdr.current_offset() as u64
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

        let mut n = 0;
        let buf_len = buf.len();

        while n < buf_len {
            let bn = std::cmp::min(self.read - self.start, buf_len - n);
            buf[n..n + bn].copy_from_slice(&self.buffer[self.start..self.start + bn]);
            self.start += bn;
            n += bn;
            if n == buf_len {
                break;
            }

            match self.rdr.read_exact(&mut self.buffer[..buf_len]) {
                Ok(_) => n,
                Err(e) => {
                    let (segment_id, offset) =
                        (self.rdr.current_segment_id(), self.rdr.current_offset());

                    let err = match e.kind() {
                        std::io::ErrorKind::UnexpectedEof => Error::LogError(LogError::Eof(n)),
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
            };

            self.read = buf_len;
            self.start = 0;
        }
        Ok(n)
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

/// `TxReader` is a public struct within the crate that is used for reading transaction records.
///
/// # Fields
/// * `r: Reader` - The `Reader` instance used to read data.
pub(crate) struct TxReader {
    r: Reader,
    err: Option<Error>,
    rec: Vec<u8>,
    max_key_size: u64,
    max_value_size: u64,
}

impl TxReader {
    /// Creates a new `TxReader` with the given `Reader`.
    ///
    /// # Arguments
    ///
    /// * `r: Reader` - The `Reader` instance to use for reading data.
    pub(crate) fn new(r: Reader, max_key_size: u64, max_value_size: u64) -> Self {
        TxReader {
            r,
            err: None,
            rec: Vec::new(),
            max_key_size,
            max_value_size,
        }
    }

    /// Reads the header of a transaction record.
    ///
    /// # Arguments
    ///
    /// * `tx: &mut TxRecord` - The transaction record to read the header into.    
    pub(crate) fn read_header(&mut self, tx: &mut TxRecord) -> Result<()> {
        let id = self.r.read_uint64()?;

        // Either the header is corrupted or we have reached the end of the file
        // and encountered the padded zeros towards the end of the file.
        if id == 0 {
            return Err(Error::InvalidTransactionRecordId);
        }

        tx.header.id = id;
        tx.header.ts = self.r.read_uint64()?;
        tx.header.version = self.r.read_uint16()?;
        tx.header.num_entries = self.r.read_uint32()?;

        let md_len = self.r.read_uint16()? as usize;
        if md_len > MAX_TX_METADATA_SIZE {
            let (segment_id, offset) = (self.r.current_segment_id(), self.r.current_offset());

            return Err(Error::LogError(Corruption(CorruptionError::new(
                std::io::ErrorKind::Other,
                Error::CorruptedTransactionHeader("metadata length exceeds maximum".to_string())
                    .to_string()
                    .as_str(),
                segment_id,
                offset,
            ))));
        }

        let mut txmd: Option<Metadata> = None;
        if md_len > 0 {
            let mut md_bs = [0; MAX_TX_METADATA_SIZE];
            self.r.read(&mut md_bs[..md_len])?;

            let metadata = Metadata::from_bytes(&md_bs)?;
            txmd = Some(metadata);
        }

        tx.header.metadata = txmd;

        Ok(())
    }

    /// Reads a transaction entry.
    fn read_entry(&mut self) -> Result<(TxEntry, u64)> {
        let md_len = self.r.read_uint16()? as usize;
        if md_len > MAX_KV_METADATA_SIZE {
            return self.corrupt_record_error("metadata length exceeds maximum");
        }

        let kvmd = if md_len > 0 {
            let md_bs = self.r.read_bytes(md_len)?;
            let metadata = Metadata::from_bytes(&md_bs)?;
            Some(metadata)
        } else {
            None
        };

        let k_len = self.r.read_uint32()? as usize;
        if k_len > self.max_key_size as usize {
            return self.corrupt_record_error("key length exceeds maximum");
        }

        let k = self.r.read_bytes(k_len)?;

        let v_len = self.r.read_uint32()? as usize;
        if v_len > self.max_value_size as usize {
            return self.corrupt_record_error("value length exceeds maximum");
        }

        let offset = self.r.offset();
        let v = self.r.read_bytes(v_len)?;
        let crc32 = self.r.read_uint32()?;

        Ok((
            TxEntry {
                metadata: kvmd,
                key: k.into(),
                value: v.into(),
                value_len: v_len as u32,
                key_len: k_len as u32,
                crc32,
            },
            offset,
        ))
    }

    fn corrupt_record_error(&self, message: &str) -> Result<(TxEntry, u64)> {
        let (segment_id, offset) = (self.r.current_segment_id(), self.r.current_offset());

        Err(Error::LogError(Corruption(CorruptionError::new(
            std::io::ErrorKind::Other,
            Error::CorruptedTransactionRecord(message.to_string())
                .to_string()
                .as_str(),
            segment_id,
            offset,
        ))))
    }

    /// Reads a transaction record into the provided `TxRecord`.
    pub(crate) fn read_into(&mut self, tx: &mut TxRecord) -> Result<HashMap<bytes::Bytes, usize>> {
        self.read_header(tx)?;

        let mut value_offsets: HashMap<bytes::Bytes, usize> = HashMap::new();
        for i in 0..tx.header.num_entries as usize {
            let (entry, offset) = self.read_entry()?;
            let key = entry.key.clone();
            tx.entries.insert(i, entry);
            value_offsets.insert(key, offset as usize);
        }

        self.verify_crc(tx)?;

        Ok(value_offsets)
    }

    /// Verifies the CRC of a transaction record.
    fn verify_crc(&mut self, tx: &TxRecord) -> Result<()> {
        let entry_crc = self.r.read_uint32()?;
        let buf = Self::serialize_tx_record(tx)?;
        let actual_crc = calculate_crc32(&buf);
        if entry_crc != actual_crc {
            let (segment_id, offset) = (self.r.current_segment_id(), self.r.current_offset());

            return Err(Error::LogError(Corruption(CorruptionError::new(
                std::io::ErrorKind::Other,
                Error::CorruptedTransactionRecord("CRC mismatch".to_string())
                    .to_string()
                    .as_str(),
                segment_id,
                offset,
            ))));
        }

        Ok(())
    }

    /// Serializes a transaction record.
    pub(crate) fn serialize_tx_record(tx: &TxRecord) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        tx.to_buf(&mut buf)?;
        Ok(buf.freeze())
    }

    pub(crate) fn serialize_tx_with_crc(tx: &TxRecord) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        tx.to_buf(&mut buf)?;
        let crc = calculate_crc32(&buf);
        buf.put_u32(crc);
        Ok(buf.freeze())
    }

    pub(crate) fn read(&mut self, max_entries: usize) -> Result<(&[u8], u64)> {
        if let Some(err) = &self.err {
            return Err(err.clone());
        }

        self.rec.clear();

        let mut tx = TxRecord::new(max_entries);
        match self.read_into(&mut tx) {
            Ok(value_offsets) => value_offsets,
            Err(e) => return Err(e),
        };

        let rec = Self::serialize_tx_with_crc(&tx)?;
        self.rec.extend(&rec);

        Ok((&self.rec, self.r.current_offset()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::log::{aof::log::Aol, Options, SegmentRef};
    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[test]
    fn reader() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let mut opts = Options::default();
        opts.max_file_size = 4;
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Test initial offset
        let sz = a.offset().unwrap();
        assert_eq!(0, sz);

        // Test appending a non-empty buffer
        let r = a.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Test appending another buffer
        let r = a.append(&[4, 5, 6, 7]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        a.close().expect("should close aol");

        let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
            .expect("should read segments");
        let sr = MultiSegmentReader::new(sr).expect("should create segment reader");

        let mut r = Reader::new_from(sr, 0, 200000);

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
                matches!(err, Error::LogError(LogError::Eof(_)))
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
        let mut opts = Options::default();
        opts.max_file_size = 40;

        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Append 10 records
        for i in 0..num_items {
            let r = a.append(&[i; REC_SIZE]); // Each record is a 4-byte array filled with `i`
            assert!(r.is_ok());
            assert_eq!(REC_SIZE, r.unwrap().1);
        }

        a.close().expect("should close aol");

        let sr = SegmentRef::read_segments_from_directory(temp_dir.path())
            .expect("should read segments");
        let sr = MultiSegmentReader::new(sr).expect("should create segment reader");

        let mut r = Reader::new_from(sr, 0, 200000);

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
                matches!(err, Error::LogError(LogError::Eof(_)))
            }
            _ => false,
        });
    }
}
