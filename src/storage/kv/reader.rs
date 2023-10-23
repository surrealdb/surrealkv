use crate::storage::kv::entry::{TxRecordEntry, TxRecordHeader, MAX_TX_METADATA_SIZE};
use crate::storage::kv::error::{Error, Result};
use crate::storage::kv::meta::Metadata;
use crate::storage::log::aol::aol::AOL;

use super::entry::TxRecord;

pub(crate) struct Reader {
    r_at: AOL,
    buffer: Vec<u8>,
    read: usize,
    start: usize,
    eof: bool,
    offset: u64,
}

impl Reader {
    pub(crate) fn new_from(r_at: AOL, off: u64, size: usize) -> Result<Self> {
        Ok(Reader {
            r_at,
            buffer: vec![0; size],
            read: 0,
            start: 0,
            eof: false,
            offset: off,
        })
    }

    fn reset(&mut self) {
        self.read = 0;
        self.eof = false;
        self.offset = 0;
        self.start = 0;
    }

    fn offset(&self) -> u64 {
        self.offset
    }

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

    pub(crate) fn read_byte(&mut self) -> Result<u8> {
        let mut b = [0; 1];
        self.read(&mut b)?;
        Ok(b[0])
    }

    pub(crate) fn read_uint64(&mut self) -> Result<u64> {
        let mut b = [0; 8];
        self.read(&mut b)?;
        Ok(u64::from_be_bytes(b))
    }

    pub(crate) fn read_uint32(&mut self) -> Result<u32> {
        let mut b = [0; 4];
        self.read(&mut b)?;
        Ok(u32::from_be_bytes(b))
    }

    pub(crate) fn read_uint16(&mut self) -> Result<u16> {
        let mut b = [0; 2];
        self.read(&mut b)?;
        Ok(u16::from_be_bytes(b))
    }
}

pub(crate) struct TxReader {
    r: Reader,
}

impl TxReader {
    pub(crate) fn new(r: Reader) -> Result<Self> {
        Ok(TxReader { r })
    }

    pub(crate) fn read_header(&mut self, tx: &mut TxRecord) -> Result<()> {
        let id = self.r.read_uint64()?;

        // Either the header is corrupted or we have reached the end of the file
        // and encountered the padded zeros towards the end of the file.
        if id == 0 {
            return Err(Error::InvalidTxRecordID);
        }

        tx.header.id = id;
        tx.header.ts = self.r.read_uint64()?;
        tx.header.version = self.r.read_uint16()?;
        tx.header.num_entries = self.r.read_uint16()?;

        let md_len = self.r.read_uint16()? as usize;
        if md_len > MAX_TX_METADATA_SIZE {
            return Err(Error::CorruptedTxHeader);
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

    pub(crate) fn read_entry(&mut self, tx: &mut TxRecordEntry) -> Result<()> {
        let md_len = self.r.read_uint16()?;
        let mut kvmd: Option<Metadata> = None;
        if md_len > 0 {
            let mut md_bs = vec![0; md_len as usize];
            self.r.read(&mut md_bs)?;

            let metadata = Metadata::from_bytes(&md_bs)?;
            kvmd = Some(metadata);
        }

        let crc = self.r.read_uint32()?;

        let k_len = self.r.read_uint32()? as usize;
        let mut k = vec![0; k_len];
        self.r.read(&mut k)?;

        let v_len = self.r.read_uint32()? as usize;
        let mut v = vec![0; v_len];
        self.r.read(&mut v)?;

        tx.crc = crc;
        tx.md = kvmd;
        tx.key = k.into();
        tx.value = v.into();
        tx.value_len = v_len as u32;
        tx.key_len = k_len as u32;

        Ok(())
    }

    pub(crate) fn read_into(&mut self, tx: &mut TxRecord) -> Result<()>{
        self.read_header(tx)?;

        for i in 0..tx.header.num_entries as usize {
            if let Some(entry) = tx.entries.get_mut(i) {
                self.read_entry(entry)?;
            } else {
                tx.entries.insert(i, TxRecordEntry::new());

                self.read_entry(tx.entries.get_mut(i).unwrap())?;
            }
        }
        Ok(())
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
    fn test_reader() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();

        // Create aol options and open a aol file
        let opts = Options::default();
        let mut a = AOL::open(temp_dir.path(), &opts).expect("should create aol");

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

        let a = AOL::open(temp_dir.path(), &opts).expect("should create aol");
        println!("offset: {:?}", a.offset());

        let mut r = Reader::new_from(a, 0, 200000).unwrap();

        let mut bs = vec![0; 2];
        let n = r.read(&mut bs).expect("should read");
        println!("bs: {:?}", bs);
        let n = r.read(&mut bs).expect("should read");
        println!("bs: {:?}", bs);
        // assert_eq!(4, n);
        // assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[..]);
    }
}
