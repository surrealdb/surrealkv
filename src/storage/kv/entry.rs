use std::io::Cursor;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::storage::index::KeyTrait;
use crate::storage::kv::calculate_crc32;
use crate::storage::kv::error::{Error, Result};
use crate::storage::kv::meta::Metadata;
use crate::storage::kv::store::MVCCStore;

pub(crate) const VALUE_LENGTH_SIZE: usize = 4; // Size of vLen in bytes
pub(crate) const VALUE_OFFSET_SIZE: usize = 8; // Size of vOff in bytes
pub(crate) const MD_SIZE: usize = 2; // Size of txmdLen and kvmdLen in bytes
pub(crate) const VERSION_SIZE: usize = 1; // Size of version in bytes
pub(crate) const MAX_KV_METADATA_SIZE: usize = 1; // Maximum size of key-value metadata in bytes
pub(crate) const MAX_TX_METADATA_SIZE: usize = 0; // Maximum size of transaction metadata in bytes

#[derive(Clone)]
pub(crate) struct Entry<'a> {
    pub(crate) key: &'a Bytes,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) value: Bytes,
    pub(crate) ts: u64,
}

impl<'a> Entry<'a> {
    pub(crate) fn new(key: &'a Bytes, value: Bytes) -> Self {
        Entry {
            key,
            metadata: None,
            value,
            ts: 0,
        }
    }

    pub(crate) fn mark_delete(&mut self) {
        if self.metadata.is_none() {
            self.metadata = Some(Metadata::new());
        }
        self.metadata.as_mut().unwrap().as_deleted(true).unwrap();
    }

    pub(crate) fn is_deleted(&self) -> bool {
        if let Some(metadata) = &self.metadata {
            metadata.deleted()
        } else {
            false
        }
    }

    pub(crate) fn set_ts(&mut self, ts: u64) {
        self.ts = ts;
    }
}

// Tx struct encoded format:
//
// +---------------------------------------------------------+
// |                     Tx (Transaction)                     |
// |---------------------------------------------------------|
// | header: TxRecordHeader                                        |
// | entries: Vec<TxRecordEntry>                                   |
// +---------------------------------------------------------+

// +---------------------------------------------------------+
// |                     TxRecordHeader                             |
// |---------------------------------------------------------|
// | id: u64                                                 |
// | ts: u64                                                 |
// | version: u16                                            |
// | metadata: Option<Metadata>                              |
// | num_entries: u16                                        |
// +---------------------------------------------------------+

// +---------------------------------------------------------+
// |                     TxRecordEntry                              |
// |---------------------------------------------------------|
// | key: Bytes                                              |
// | key_len: u32                                            |
// | md: Option<Metadata>                                    |
// | value_len: u32                                          |
// | value: Bytes                                            |
// +---------------------------------------------------------+
//
//
// Tx encoded format:
//
//   |----------------------------------|
//   |   TxRecordHeader   |     TxRecordEntry[]    |
//   |--------------|-------------------|
//   | id(8) | ts(8) | version(2) | num_entries(2) | metadata_len(2) | metadata | ...entries... |
//   |----------------------------------|
//
// TxRecordHeader struct encoded format:
//
//   |------------------------------------------|
//   |   id(8)   |   ts(8)   | version(2) | num_entries(2) | metadata_len(2) | metadata |
//   |-----------|-----------|------------|----------------|----------------|----------|
//
// TxRecordEntry struct encoded format:
//
//   |----------------------------------|
//   | crc(4) | key_len(4) | key | value_len(4) | value | metadata_len(4) | metadata |
//   |----------------------------------|

pub(crate) struct TxRecord {
    header: TxRecordHeader,
    entries: Vec<TxRecordEntry>,
}

impl TxRecord {
    pub(crate) fn new() -> Self {
        TxRecord {
            header: TxRecordHeader::new(),
            entries: Vec::new(),
        }
    }

    pub(crate) fn new_from_entries(entries: Vec<Entry>, tx_id: u64) -> Self {
        let mut tx_record = TxRecord::new();
        tx_record.header.id = tx_id;

        for entry in entries {
            tx_record.add_entry(entry);
        }
        tx_record
    }

    pub(crate) fn add_entry(&mut self, entry: Entry) {
        let crc = calculate_crc32(entry.key.as_ref(), entry.value.as_ref());
        let tx_record_entry = TxRecordEntry {
            crc: crc,
            key: entry.key.clone(),
            key_len: entry.key.len() as u32,
            md: entry.metadata,
            value_len: entry.value.len() as u32,
            value: entry.value.clone(),
        };
        self.entries.push(tx_record_entry);
        self.header.num_entries += 1;
    }

    // TODO!!: use same allocated buffer for all tx encoding
    pub(crate) fn encode(&self, buf: &mut BytesMut) {
        // Encode header
        self.header.encode(buf);

        // Encode entries
        for entry in &self.entries {
            if let Some(metadata) = &entry.md {
                let md_bytes = metadata.bytes();
                let md_len = md_bytes.len() as u16;
                buf.put_u16(md_len);
                buf.put(md_bytes);
            } else {
                buf.put_u16(0);
            }
            buf.put_u32(entry.crc);
            buf.put_u32(entry.key_len);
            buf.put(entry.key.as_ref());
            buf.put_u32(entry.value_len);
            buf.put(entry.value.as_ref());
        }
    }
}

#[derive(Debug)]
pub(crate) struct TxRecordHeader {
    pub(crate) id: u64,
    pub(crate) ts: u64,
    pub(crate) version: u16,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) num_entries: u16,
}

impl TxRecordHeader {
    pub(crate) fn new() -> Self {
        TxRecordHeader {
            id: 0,
            ts: 0,
            version: 0,
            metadata: None,
            num_entries: 0,
        }
    }

    pub(crate) fn set_id(&mut self, id: u64) {
        self.id = id;
    }

    pub(crate) fn set_ts(&mut self, ts: u64) {
        self.ts = ts;
    }

    pub(crate) fn set_version(&mut self, version: u16) {
        self.version = version;
    }

    pub(crate) fn set_metadata(&mut self, metadata: Option<Metadata>) {
        self.metadata = metadata;
    }
}

#[derive(Debug)]
pub(crate) struct TxRecordEntry {
    pub(crate) crc: u32,
    pub(crate) key: Bytes,
    pub(crate) key_len: u32,
    pub(crate) md: Option<Metadata>,
    pub(crate) value_len: u32,
    pub(crate) value: Bytes,
}

impl TxRecordHeader {
    pub(crate) fn encode(&self, buf: &mut BytesMut) {
        let (md_len, md_bytes) = match &self.metadata {
            Some(metadata) => {
                let md_bytes = metadata.bytes();
                let md_len = md_bytes.len() as u16;
                (md_len, md_bytes)
            }
            None => (0, Bytes::new()),
        };

        // tx_id(8) + ts(8) + version(2) + num_entries(2) + meta_data_len(2) + metadata
        // let mut buf = BytesMut::with_capacity(20 + md_len as usize + md_bytes.len());
        buf.put_u64(self.id);
        buf.put_u64(self.ts);
        buf.put_u16(self.version);
        buf.put_u16(self.num_entries);
        buf.put_u16(md_len);
        if md_len > 0 {
            buf.put(md_bytes);
        }
    }

    pub(crate) fn decode(&mut self, encoded_bytes: &Bytes) -> Result<()> {
        let mut cursor = Cursor::new(encoded_bytes);
        self.id = cursor.get_u64();
        self.ts = cursor.get_u64();
        self.version = cursor.get_u16();

        let md_len = cursor.get_u16() as usize;
        if md_len > 0 {
            let metadata_bytes = cursor.get_ref()[cursor.position() as usize..][..md_len].as_ref();
            cursor.advance(md_len);
            let metadata = Metadata::from_bytes(&metadata_bytes)?;
            self.metadata = Some(metadata);
        } else {
            self.metadata = None;
        }

        self.num_entries = cursor.get_u16();

        Ok(())
    }
}

/// Value reference implementation.
pub struct ValueRef<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    pub(crate) version: u8,
    pub(crate) ts: u64,
    pub(crate) value_offset: i64,
    pub(crate) value_length: usize,
    pub(crate) transaction_metadata: Option<Metadata>,
    pub(crate) key_value_metadata: Option<Metadata>,
    /// The underlying store for the transaction.
    store: Arc<MVCCStore<P, V>>,
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> ValueRef<P, V> {
    fn resolve(&self) -> Result<Vec<u8>> {
        // Implement the resolve functionality.
        unimplemented!("resolve");
    }

    fn transaction_id(&self) -> u64 {
        self.ts
    }

    fn transaction_metadata(&self) -> Option<&Metadata> {
        self.transaction_metadata.as_ref()
    }

    pub(crate) fn key_value_metadata(&self) -> Option<&Metadata> {
        self.key_value_metadata.as_ref()
    }

    fn length(&self) -> usize {
        self.value_length
    }
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> ValueRef<P, V> {
    pub(crate) fn new(store: Arc<MVCCStore<P, V>>) -> Self {
        ValueRef {
            version: 0,
            ts: 0,
            value_offset: 0,
            value_length: 0,
            transaction_metadata: None,
            key_value_metadata: None,
            store,
        }
    }

    /// Encode the valueRef into a byte representation.
    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Encode version, value length, and value offset
        buf.extend_from_slice(&(self.version as u8).to_be_bytes());
        buf.extend_from_slice(&(self.value_length as u32).to_be_bytes());
        buf.extend_from_slice(&(self.value_offset as i64).to_be_bytes());

        // Encode transaction metadata
        if let Some(tx_metadata) = &self.transaction_metadata {
            let tx_metadata_bytes = tx_metadata.bytes();
            let tx_metadata_len = tx_metadata_bytes.len() as u16;
            buf.extend_from_slice(&(tx_metadata_len as u16).to_be_bytes());
            buf.extend_from_slice(&tx_metadata_bytes);
        } else {
            buf.extend_from_slice(&0u16.to_be_bytes());
        }

        // Encode key-value metadata
        if let Some(kv_metadata) = &self.key_value_metadata {
            let kv_metadata_bytes = kv_metadata.bytes();
            let kv_metadata_len = kv_metadata_bytes.len() as u16;
            buf.extend_from_slice(&(kv_metadata_len as u16).to_be_bytes());
            buf.extend_from_slice(&kv_metadata_bytes);
        } else {
            buf.extend_from_slice(&0u16.to_be_bytes());
        }

        buf.freeze()
    }

    // TODO: Draw ascii diagram for decode format
    /// Decode the byte representation into a valueRef.
    pub(crate) fn decode(&mut self, ts: u64, encoded_bytes: &Bytes) -> Result<()> {
        let mut cursor = Cursor::new(encoded_bytes);

        // Set ts
        self.ts = ts;

        // Decode version, value length, and value offset
        self.version = cursor.get_u8();
        self.value_length = cursor.get_u32() as usize;
        self.value_offset = cursor.get_i64();

        // Decode transaction metadata
        if encoded_bytes.len() < cursor.position() as usize + 2 * MD_SIZE {
            return Err(Error::CorruptedIndex);
        }
        let tx_metadata_len = cursor.get_u16() as usize;
        if tx_metadata_len > 0 {
            if encoded_bytes.len() < cursor.position() as usize + tx_metadata_len {
                return Err(Error::CorruptedIndex);
            }
            let tx_metadata_bytes =
                cursor.get_ref()[cursor.position() as usize..][..tx_metadata_len].as_ref();
            cursor.advance(tx_metadata_len);
            self.transaction_metadata = Some(Metadata::from_bytes(tx_metadata_bytes)?);
        } else {
            self.transaction_metadata = None;
        }

        // Decode key-value metadata
        if encoded_bytes.len() < cursor.position() as usize + MD_SIZE {
            return Err(Error::CorruptedIndex);
        }
        let kv_metadata_len = cursor.get_u16() as usize;
        if kv_metadata_len > 0 {
            if encoded_bytes.len() < cursor.position() as usize + kv_metadata_len {
                return Err(Error::CorruptedIndex);
            }
            let kv_metadata_bytes =
                cursor.get_ref()[cursor.position() as usize..][..kv_metadata_len].as_ref();
            cursor.advance(kv_metadata_len);
            self.key_value_metadata = Some(Metadata::from_bytes(kv_metadata_bytes)?);
        } else {
            self.key_value_metadata = None;
        }

        // Ensure all the data is read
        if cursor.position() as usize != encoded_bytes.len() {
            return Err(Error::CorruptedIndex);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::index::VectorKey;
    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::MVCCStore;
    use crate::storage::kv::util::NoopValue;

    #[test]
    fn test_encode_decode() {
        // Create a sample valueRef instance
        let opts = Options::new();
        let store = Arc::new(MVCCStore::<VectorKey, NoopValue>::new(opts));

        let mut txmd = Metadata::new();
        txmd.as_deleted(true).expect("failed to set deleted");
        let mut kvmd = Metadata::new();
        kvmd.as_deleted(true).expect("failed to set deleted");

        let mut value_ref = ValueRef::new(store.clone());
        value_ref.version = 42;
        value_ref.value_length = 100;
        value_ref.value_offset = 200;
        value_ref.transaction_metadata = Some(txmd);
        value_ref.key_value_metadata = Some(kvmd);

        // Encode the valueRef
        let encoded_bytes = value_ref.encode();

        // Decode the encoded bytes into a new valueRef
        let mut decoded_value_ref = ValueRef::new(store);
        decoded_value_ref.decode(0, &encoded_bytes).unwrap();

        // Check if the decoded valueRef matches the original
        assert_eq!(decoded_value_ref.version, value_ref.version);
        assert_eq!(decoded_value_ref.value_length, value_ref.value_length);
        assert_eq!(decoded_value_ref.value_offset, value_ref.value_offset);
        assert_eq!(
            decoded_value_ref.transaction_metadata.unwrap().deleted(),
            value_ref.transaction_metadata.unwrap().deleted()
        );
        assert_eq!(
            decoded_value_ref.key_value_metadata.unwrap().deleted(),
            value_ref.key_value_metadata.unwrap().deleted()
        );
    }
}
