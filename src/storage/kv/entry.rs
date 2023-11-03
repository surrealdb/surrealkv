use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::storage::index::KeyTrait;
use crate::storage::kv::error::{Error, Result};
use crate::storage::kv::meta::Metadata;
use crate::storage::kv::store::Core;
use crate::storage::kv::util::calculate_crc32;

pub(crate) const MD_SIZE: usize = 2; // Size of txmdLen and kvmdLen in bytes
pub(crate) const VERSION_SIZE: usize = 1; // Size of version in bytes
pub(crate) const VALUE_LENGTH_SIZE: usize = 4; // Size of vLen in bytes
pub(crate) const VALUE_OFFSET_SIZE: usize = 8; // Size of vOff in bytes
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
// |                     Tx (Transaction)                    |
// |---------------------------------------------------------|
// | header: TxRecordHeader                                  |
// | entries: Vec<TxRecordEntry>                             |
// +---------------------------------------------------------+

// +---------------------------------------------------------+
// |                     TxRecordHeader                      |
// |---------------------------------------------------------|
// | id: u64                                                 |
// | ts: u64                                                 |
// | version: u16                                            |
// | metadata: Option<Metadata>                              |
// | num_entries: u16                                        |
// +---------------------------------------------------------+

// +---------------------------------------------------------+
// |                     TxRecordEntry                       |
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
//   |---------------------------------------------|
//   |   TxRecordHeader   |     TxRecordEntry[]    |
//   |-------|-------|------------|----------------|-----------------|----------|---------------|
//   | id(8) | ts(8) | version(2) | num_entries(2) | metadata_len(2) | metadata | ...entries... |
//   |-------|-------|------------|----------------|-----------------|----------|---------------|
//
// TxRecordHeader struct encoded format:
//
//   |-----------|-----------|------------|----------------|-----------------|-----------|
//   |   id(8)   |   ts(8)   | version(2) | num_entries(2) | metadata_len(2) | metadata  |
//   |-----------|-----------|------------|----------------|-----------------|-----------|
//
// TxRecordEntry struct encoded format:
//
//   |---------------------------|--------------|-------|-----------------|----------|
//   | crc(4) | key_len(4) | key | value_len(4) | value | metadata_len(4) | metadata |
//   |---------------------------|--------------|-------|-----------------|----------|
//
#[derive(Debug)]
pub(crate) struct TxRecord {
    pub(crate) header: TxRecordHeader,
    pub(crate) entries: Vec<TxRecordEntry>,
}

impl TxRecord {
    pub(crate) fn new(max_entries: usize) -> Self {
        TxRecord {
            header: TxRecordHeader::new(),
            entries: Vec::with_capacity(max_entries),
        }
    }

    pub(crate) fn reset(&mut self) {
        self.header.reset();
        self.entries.clear();
    }

    pub(crate) fn new_with_entries(entries: Vec<Entry>, tx_id: u64, commit_ts: u64) -> Self {
        let mut tx_record = TxRecord::new(entries.len());
        tx_record.header.id = tx_id;
        tx_record.header.ts = commit_ts;

        for entry in entries {
            tx_record.add_entry(entry);
        }
        tx_record
    }

    pub(crate) fn add_entry(&mut self, entry: Entry) {
        let crc = calculate_crc32(entry.key.as_ref(), entry.value.as_ref());
        let tx_record_entry = TxRecordEntry {
            crc,
            key: entry.key.clone(),
            key_len: entry.key.len() as u32,
            metadata: entry.metadata,
            value_len: entry.value.len() as u32,
            value: entry.value,
        };
        self.entries.push(tx_record_entry);
        self.header.num_entries += 1;
    }

    // TODO!!: use same allocated buffer for all tx encoding
    pub(crate) fn encode(
        &self,
        buf: &mut BytesMut,
        offset_tracker: &mut HashMap<Bytes, usize>,
    ) -> Result<()> {
        // Encode header
        self.header.encode(buf);

        // Encode entries and store offsets
        let mut offset = buf.len();

        for entry in &self.entries {
            // Store the offset for the current entry
            offset_tracker.insert(entry.key.clone(), offset);

            // Encode metadata, if present
            if let Some(metadata) = &entry.metadata {
                let md_bytes = metadata.bytes();
                let md_len = md_bytes.len() as u16;
                buf.put_u16(md_len);
                buf.put(md_bytes);
            } else {
                buf.put_u16(0);
            }

            // Encode CRC, key length, key, value length, and value
            buf.put_u32(entry.crc);
            buf.put_u32(entry.key_len);
            buf.put(entry.key.as_ref());
            buf.put_u32(entry.value_len);
            buf.put(entry.value.as_ref());

            offset += buf.len();
        }

        Ok(())
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

    pub(crate) fn reset(&mut self) {
        self.id = 0;
        self.ts = 0;
        self.version = 0;
        self.metadata = None;
        self.num_entries = 0;
    }

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
            let metadata = Metadata::from_bytes(metadata_bytes)?;
            self.metadata = Some(metadata);
        } else {
            self.metadata = None;
        }

        self.num_entries = cursor.get_u16();

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct TxRecordEntry {
    pub(crate) crc: u32,
    pub(crate) key: Bytes,
    pub(crate) key_len: u32,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) value_len: u32,
    pub(crate) value: Bytes,
}

impl TxRecordEntry {
    pub(crate) fn new() -> Self {
        TxRecordEntry {
            crc: 0,
            key: Bytes::new(),
            key_len: 0,
            metadata: None,
            value_len: 0,
            value: Bytes::new(),
        }
    }
}

/// Value reference implementation.
pub struct ValueRef<P: KeyTrait> {
    pub(crate) version: u8,
    pub(crate) flag: u8,
    pub(crate) ts: u64,
    pub(crate) value_offset: u64,
    pub(crate) value_length: usize,
    pub(crate) value: Option<Bytes>,
    pub(crate) key_value_metadata: Option<Metadata>,
    /// The underlying store for the transaction.
    store: Arc<Core<P>>,
}

impl<P: KeyTrait> ValueRef<P> {
    fn resolve(&self) -> Result<Vec<u8>> {
        // Implement the resolve functionality.
        unimplemented!("resolve");
    }

    fn transaction_id(&self) -> u64 {
        self.ts
    }

    pub(crate) fn key_value_metadata(&self) -> Option<&Metadata> {
        self.key_value_metadata.as_ref()
    }

    fn length(&self) -> usize {
        self.value_length
    }
}

impl<P: KeyTrait> ValueRef<P> {
    pub(crate) fn new(store: Arc<Core<P>>) -> Self {
        ValueRef {
            version: 0,
            ts: 0,
            flag: 0,
            value_offset: 0,
            value_length: 0,
            value: None,
            key_value_metadata: None,
            store,
        }
    }

    /// Encode the valueRef into a byte representation.
    pub(crate) fn encode(
        key: &Bytes,
        value: &Bytes,
        metadata: Option<&Metadata>,
        value_offsets: &HashMap<bytes::Bytes, usize>,
        max_value_threshold: usize,
    ) -> Bytes {
        let mut buf = BytesMut::new();

        if value.len() <= max_value_threshold {
            buf.put_u8(1);
            buf.put_u32(value.len() as u32);
            buf.put(value.as_ref());
        } else {
            buf.put_u8(0);
            buf.put_u32(value.len() as u32);
            let val_off = value_offsets.get(key).unwrap();
            buf.put_u64(*val_off as u64);
        }

        if let Some(metadata) = &metadata {
            let md_bytes = metadata.bytes();
            let md_len = md_bytes.len() as u16;
            buf.put_u16(md_len);
            buf.put(md_bytes);
        } else {
            buf.put_u16(0);
        }
        buf.freeze()
    }

    // TODO: Draw ascii diagram for decode format
    /// Decode the byte representation into a valueRef.
    pub(crate) fn decode(&mut self, ts: u64, encoded_bytes: &Bytes) -> Result<()> {
        let mut cursor = Cursor::new(encoded_bytes);

        // Set ts
        self.ts = ts;

        // Read flag which indicates if value is inlined or not
        self.flag = cursor.get_u8();

        // Decode value length and value
        self.value_length = cursor.get_u32() as usize;

        if self.flag == 1 {
            let value_bytes =
                cursor.get_ref()[cursor.position() as usize..][..self.value_length].as_ref();
            cursor.advance(self.value_length);

            self.value = Some(Bytes::copy_from_slice(value_bytes));
        } else {
            // Decode version, value length, and value offset
            self.value_offset = cursor.get_u64();
        }

        // Decode key-value metadata
        if encoded_bytes.len() < cursor.position() as usize + MD_SIZE {
            return Err(Error::CorruptedIndex);
        }

        let kv_metadata_len = cursor.get_u16() as usize;
        if kv_metadata_len > 0 {
            if kv_metadata_len > MAX_KV_METADATA_SIZE {
                return Err(Error::CorruptedIndex);
            }
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
    use crate::storage::kv::store::Core;

    #[test]
    fn test_encode_decode() {
        // Create a sample valueRef instance
        let opts = Options::new();
        let store = Arc::new(Core::<VectorKey>::new(opts).expect("failed to create store"));

        let mut txmd = Metadata::new();
        txmd.as_deleted(true).expect("failed to set deleted");
        let mut kvmd = Metadata::new();
        kvmd.as_deleted(true).expect("failed to set deleted");

        let mut value_ref = ValueRef::new(store);
        value_ref.version = 42;
        value_ref.value_length = 100;
        value_ref.value_offset = 200;
        value_ref.key_value_metadata = Some(kvmd);

        // // Encode the valueRef
        // let encoded_bytes = value_ref.encode();

        // // Decode the encoded bytes into a new valueRef
        // let mut decoded_value_ref = ValueRef::new(store);
        // decoded_value_ref.decode(0, &encoded_bytes).unwrap();

        // // Check if the decoded valueRef matches the original
        // assert_eq!(decoded_value_ref.version, value_ref.version);
        // assert_eq!(decoded_value_ref.value_length, value_ref.value_length);
        // assert_eq!(decoded_value_ref.value_offset, value_ref.value_offset);
        // assert_eq!(
        //     decoded_value_ref.key_value_metadata.unwrap().deleted(),
        //     value_ref.key_value_metadata.unwrap().deleted()
        // );
    }
}
