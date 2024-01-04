use bytes::{Bytes, BytesMut};
use hashbrown::HashMap;

use crate::storage::{
    kv::{
        entry::{TxEntry, MAX_TX_METADATA_SIZE},
        error::{Error, Result},
        meta::Metadata,
    },
    log::aof::log::Aol,
};

use super::entry::TxRecord;
use super::util::calculate_crc32;

/// `Reader` is a generic reader for reading data from an Aol. It is used
/// by the `TxReader` to read data from the Aol source.
///
/// # Fields
/// * `r_at: Aol` - Represents the current position in the Aol source.
/// * `buffer: Vec<u8>` - A buffer that temporarily holds data read from the source.
/// * `read: usize` - The number of bytes read from the source.
/// * `start: usize` - The starting position for reading data.
/// * `eof: bool` - A flag indicating if the end of the file/data source has been reached.
/// * `offset: u64` - The offset from the beginning of the file/data source.
pub(crate) struct Reader {
    r_at: Aol,
    buffer: Vec<u8>,
    read: usize,
    start: usize,
    eof: bool,
    offset: u64,
}

impl Reader {
    /// Creates a new `Reader` with the given `r_at`, `off`, and `size`.
    ///
    /// # Arguments
    ///
    /// * `r_at: Aol` - The current position in the data source.
    /// * `off: u64` - The offset from the beginning of the file/data source.
    /// * `size: usize` - The size of the buffer.
    pub(crate) fn new_from(r_at: Aol, off: u64, size: usize) -> Result<Self> {
        Ok(Reader {
            r_at,
            buffer: vec![0; size],
            read: 0,
            start: 0,
            eof: false,
            offset: off,
        })
    }

    /// Returns the current offset of the `Reader`.
    fn offset(&self) -> u64 {
        self.offset
    }

    /// Reads data into the provided buffer.
    ///
    /// # Arguments
    ///
    /// * `buf: &mut [u8]` - The buffer to read data into.
    pub(crate) fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
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

            if self.eof {
                return Ok(n);
            }

            let rn = self
                .r_at
                .read_at(&mut self.buffer[..buf_len], self.offset)?;
            self.read = rn;
            self.start = 0;
            self.offset += rn as u64;

            if rn == 0 {
                self.eof = true;
                continue;
            }
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
}

impl TxReader {
    /// Creates a new `TxReader` with the given `Reader`.
    ///
    /// # Arguments
    ///
    /// * `r: Reader` - The `Reader` instance to use for reading data.
    pub(crate) fn new(r: Reader) -> Result<Self> {
        Ok(TxReader { r })
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
        tx.header.lsn = self.r.read_uint64()?;
        tx.header.ts = self.r.read_uint64()?;
        tx.header.version = self.r.read_uint16()?;
        tx.header.num_entries = self.r.read_uint32()?;

        let md_len = self.r.read_uint16()? as usize;
        if md_len > MAX_TX_METADATA_SIZE {
            return Err(Error::CorruptedTransactionHeader);
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
        let md_len = self.r.read_uint16()?;
        let kvmd = if md_len > 0 {
            let md_bs = self.r.read_bytes(md_len as usize)?;
            let metadata = Metadata::from_bytes(&md_bs)?;
            Some(metadata)
        } else {
            None
        };

        let k_len = self.r.read_uint32()? as usize;
        let k = self.r.read_bytes(k_len)?;

        let v_len = self.r.read_uint32()? as usize;
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

    /// Reads a transaction record into the provided `TxRecord`.
    ///
    /// # Arguments
    ///
    /// * `tx: &mut TxRecord` - The `TxRecord` to read data into.
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
    ///
    /// # Arguments
    ///
    /// * `tx: &TxRecord` - The `TxRecord` to verify the CRC of.    
    fn verify_crc(&mut self, tx: &TxRecord) -> Result<()> {
        let entry_crc = self.r.read_uint32()?;
        let buf = Self::serialize_tx_record(tx)?;
        let actual_crc = calculate_crc32(&buf);
        if entry_crc != actual_crc {
            return Err(Error::CorruptedTransactionRecord);
        }

        Ok(())
    }

    /// Serializes a transaction record.
    ///
    /// # Arguments
    ///
    /// * `tx: &TxRecord` - The `TxRecord` to serialize.
    fn serialize_tx_record(tx: &TxRecord) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        tx.to_buf(&mut buf)?;
        Ok(buf.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::log::Options;
    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[test]
    fn reader() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options::default();
        let mut a = Aol::open(temp_dir.path(), &opts).expect("should create aol");

        // Test initial offset
        let sz = a.offset().unwrap();
        assert_eq!(0, sz);

        // Test appending a non-empty buffer
        let r = a.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Test appending another buffer
        let r = a.append(&[4, 5, 6, 7, 8, 9, 10, 11]);
        assert!(r.is_ok());
        assert_eq!(8, r.unwrap().1);

        a.close().expect("should close aol");

        let a = Aol::open(temp_dir.path(), &opts).expect("should create aol");
        let mut r = Reader::new_from(a, 0, 200000).unwrap();

        let mut bs = vec![0; 4];
        let n = r.read(&mut bs).expect("should read");
        assert_eq!(4, n);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[..]);
    }
}
