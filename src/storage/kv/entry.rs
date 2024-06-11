use hashbrown::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::storage::{
    kv::error::{Error, Result},
    kv::meta::Metadata,
    kv::store::Core,
    kv::util::calculate_crc32_combined,
};

pub(crate) const MD_SIZE: usize = 1; // Size of txmdLen and kvmdLen in bytes
pub(crate) const MAX_KV_METADATA_SIZE: usize = 1; // Maximum size of key-value metadata in bytes
pub(crate) const RECORD_VERSION: u16 = 1; // Version of the transaction header

#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) key: Bytes,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) value: Bytes,
    pub(crate) ts: u64,
}

impl Entry {
    pub(crate) fn new(key: &[u8], value: &[u8]) -> Self {
        Entry {
            key: Bytes::copy_from_slice(key),
            metadata: None,
            value: Bytes::copy_from_slice(value),
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

    pub(crate) fn crc32(&self) -> u32 {
        calculate_crc32_combined(&self.key, &self.value)
    }
}

// Record encoded format:
//
//   +---------------------------------------------------------+
//   |                        Record                           |
//   |---------------------------------------------------------|
//   | crc32: u32                                              |
//   | version: u16                                            |
//   | tx_id: u64                                              |
//   | ts: u64                                                 |
//   | md: Option<Metadata>                                    |
//   | key_len: u32                                            |
//   | key: Bytes                                              |
//   | value_len: u32                                          |
//   | value: Bytes                                            |
//   +---------------------------------------------------------+
//
//
// Record encoded format:
//
//   |----------|------------|------------|---------|-----------------|------------|------------|-----|--------------|-------|
//   | crc32(4) | version(2) |  tx_id(8)  |  ts(8)  | metadata_len(2) |  metadata  | key_len(4) | key | value_len(4) | value |
//   |----------|------------|------------|---------|-----------------|------------|------------|-----|--------------|-------|
//
#[derive(Debug)]
pub(crate) struct Records {
    pub(crate) entries: Vec<Record>,
}

impl Records {
    pub(crate) fn new(max_entries: usize) -> Self {
        Records {
            entries: Vec::with_capacity(max_entries),
        }
    }

    pub(crate) fn new_with_entries(entries: Vec<Entry>, tx_id: u64, commit_ts: u64) -> Self {
        let mut tx_record = Records::new(entries.len());

        for entry in entries {
            tx_record.add_entry(entry, tx_id, commit_ts);
        }
        tx_record
    }

    pub(crate) fn add_entry(&mut self, entry: Entry, tx_id: u64, commit_ts: u64) {
        let crc32 = entry.crc32();
        let tx_record_entry = Record {
            id: tx_id,
            ts: commit_ts,
            version: RECORD_VERSION,
            key_len: entry.key.len() as u32,
            key: entry.key,
            metadata: entry.metadata,
            value_len: entry.value.len() as u32,
            value: entry.value,
            crc32: crc32,
        };
        self.entries.push(tx_record_entry);
    }

    pub(crate) fn encode(
        &self,
        buf: &mut BytesMut,
        current_offset: u64,
        offset_tracker: &mut HashMap<Bytes, usize>,
    ) -> Result<()> {
        // Encode entries and store offsets
        for entry in &self.entries {
            let mut offset = entry.encode(buf)?;
            offset += current_offset as usize;

            // Store the offset for the current entry
            offset_tracker.insert(entry.key.clone(), offset);
        }

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Record {
    pub(crate) id: u64,
    pub(crate) ts: u64,
    pub(crate) version: u16,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) key: Bytes,
    pub(crate) key_len: u32,
    pub(crate) value_len: u32,
    pub(crate) value: Bytes,
    pub(crate) crc32: u32,
}

impl Record {
    pub(crate) fn new() -> Self {
        Record {
            id: 0,
            ts: 0,
            version: 0,
            metadata: None,
            key: Bytes::new(),
            key_len: 0,
            value_len: 0,
            value: Bytes::new(),
            crc32: 0,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.id = 0;
        self.ts = 0;
        self.version = 0;
        self.metadata = None;
        self.key.clear();
        self.key_len = 0;
        self.value_len = 0;
        self.value.clear();
        self.crc32 = 0;
    }

    pub(crate) fn from_entry(entry: Entry, tx_id: u64, commit_ts: u64) -> Record {
        let crc32 = entry.crc32();
        Record {
            id: tx_id,
            ts: commit_ts,
            version: RECORD_VERSION,
            key_len: entry.key.len() as u32,
            key: entry.key,
            metadata: entry.metadata,
            value_len: entry.value.len() as u32,
            value: entry.value,
            crc32: crc32,
        }
    }

    pub(crate) fn encode(&self, buf: &mut BytesMut) -> Result<usize> {
        // This function encodes an Record into a buffer. The encoding format is as follows:
        // - CRC32 Checksum (4 bytes)
        // - Version (2 bytes)
        // - Transaction ID (8 bytes)
        // - Timestamp (8 bytes)
        // - Metadata Length (2 bytes)
        // - Metadata (variable length, defined by Metadata Length)
        // - Key Length (4 bytes)
        // - Key (variable length, defined by Key Length)
        // - Value Length (4 bytes)
        // - Value (variable length, defined by Value Length)
        // The function returns the offset position in the buffer after the Value field.
        buf.put_u32(self.crc32);
        buf.put_u16(self.version);
        buf.put_u64(self.id);
        buf.put_u64(self.ts);

        if let Some(metadata) = &self.metadata {
            let md_bytes = metadata.to_bytes();
            let md_len = md_bytes.len() as u16;
            buf.put_u16(md_len);
            buf.put(md_bytes);
        } else {
            buf.put_u16(0);
        }

        buf.put_u32(self.key_len);
        buf.put(self.key.as_ref());
        buf.put_u32(self.value_len);
        let offset = buf.len();
        buf.put(self.value.as_ref());

        Ok(offset)
    }
}

pub(crate) trait Value {
    fn resolve(&self) -> Result<Vec<u8>>;
    fn ts(&self) -> u64;
    fn key_value_metadata(&self) -> Option<&Metadata>;
    fn length(&self) -> usize;
}

/// Value reference implementation.
pub struct ValueRef {
    pub(crate) flag: u8,
    pub(crate) ts: u64,
    pub(crate) value_length: usize,
    pub(crate) value_offset: Option<u64>,
    pub(crate) value: Option<Bytes>,
    pub(crate) key_value_metadata: Option<Metadata>,
    /// The underlying store for the transaction.
    store: Arc<Core>,
}

impl Value for ValueRef {
    /// Resolves the value associated with this instance.
    /// If the value is present, it returns a cloned vector of the value.
    /// If the value offset is present, it reads the value from the offset in the commit log.
    fn resolve(&self) -> Result<Vec<u8>> {
        // Check if the value is present directly
        if let Some(value) = &self.value {
            Ok(value.to_vec())
        } else if let Some(value_offset) = self.value_offset {
            // Resolve from the specified offset
            self.resolve_from_offset(value_offset)
        } else {
            // If neither value nor offset is present, return an error
            Err(Error::EmptyValue)
        }
    }

    fn ts(&self) -> u64 {
        self.ts
    }

    fn key_value_metadata(&self) -> Option<&Metadata> {
        self.key_value_metadata.as_ref()
    }

    fn length(&self) -> usize {
        self.value_length
    }
}

impl ValueRef {
    pub(crate) fn new(store: Arc<Core>) -> Self {
        ValueRef {
            ts: 0,
            flag: 0,
            value_offset: None,
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
            buf.put_u8(1); // swizzle flag to indicate value is inlined or stored in log
            buf.put_u32(value.len() as u32);
            let val_off = value_offsets.get(key).unwrap();
            buf.put_u64(*val_off as u64);
            buf.put(value.as_ref());
        } else {
            buf.put_u8(0);
            buf.put_u32(value.len() as u32);
            let val_off = value_offsets.get(key).unwrap();
            buf.put_u64(*val_off as u64);
        }

        if let Some(metadata) = &metadata {
            let md_bytes = metadata.to_bytes();
            let md_len = md_bytes.len() as u16;
            buf.put_u16(md_len);
            buf.put(md_bytes);
        } else {
            buf.put_u16(0);
        }
        buf.freeze()
    }

    /// Encode the valueRef into an in-memory byte representation.
    pub(crate) fn encode_mem(value: &Bytes, metadata: Option<&Metadata>) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_u8(1); // swizzle flag to indicate value is inlined or stored in log
        buf.put_u32(value.len() as u32);
        buf.put_u64(0);
        buf.put(value.as_ref());

        if let Some(metadata) = &metadata {
            let md_bytes = metadata.to_bytes();
            let md_len = md_bytes.len() as u16;
            buf.put_u16(md_len);
            buf.put(md_bytes);
        } else {
            buf.put_u16(0);
        }
        buf.freeze()
    }

    /// Decode the byte representation into a valueRef.
    pub(crate) fn decode(&mut self, ts: u64, encoded_bytes: &Bytes) -> Result<()> {
        let mut cursor = Cursor::new(encoded_bytes);

        // Set ts
        self.ts = ts;

        // Read flag which indicates if value is inlined or not
        self.flag = cursor.get_u8();

        // Decode value length and value
        self.value_length = cursor.get_u32() as usize;
        self.value_offset = Some(cursor.get_u64());

        if self.flag == 1 {
            let value_bytes =
                cursor.get_ref()[cursor.position() as usize..][..self.value_length].as_ref();
            cursor.advance(self.value_length);

            self.value = Some(Bytes::copy_from_slice(value_bytes));
        } else {
            // Decode version, value length, and value offset
            // self.value_offset = Some(cursor.get_u64());
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

    /// Resolves the value from the given offset in the commit log.
    /// If the offset exists in the value cache, it returns the cached value.
    /// Otherwise, it reads the value from the commit log, caches it, and returns it.
    fn resolve_from_offset(&self, value_offset: u64) -> Result<Vec<u8>> {
        // Check if the offset exists in value_cache and return if found
        if let Some(value) = self.store.value_cache.get(&value_offset) {
            return Ok(value.to_vec());
        }

        // Read the value from the commit log at the specified offset
        let mut buf = vec![0; self.value_length];
        let vlog = self.store.clog.as_ref().unwrap().read();
        vlog.read_at(&mut buf, value_offset)?;

        // Store the offset and value in value_cache
        self.store
            .value_cache
            .insert(value_offset, Bytes::from(buf.clone()));

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::Store;

    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[tokio::test]
    async fn encode_decode() {
        // Create a sample valueRef instance
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new Core instance with VariableKey as the key type
        let store = Store::new(opts).expect("should create store");

        let mut txmd = Metadata::new();
        txmd.as_deleted(true).expect("failed to set deleted");
        let mut kvmd = Metadata::new();
        kvmd.as_deleted(true).expect("failed to set deleted");

        let mut value_ref = ValueRef::new(store.inner.as_ref().unwrap().core.clone());
        value_ref.value_length = 100;
        value_ref.value_offset = Some(200);
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

    #[tokio::test]
    async fn txn_with_value_read_from_clog() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 2;

        // Create a new Core instance with VariableKey as the key type
        let store = Store::new(opts).expect("should create store");

        // Define test keys and value
        let key1 = Bytes::from("foo1");
        let key2 = Bytes::from("foo2");
        let key3 = Bytes::from("foo3");
        let value = Bytes::from("bar");

        {
            // Start a new write transaction (txn)
            let mut txn = store.begin().unwrap();

            // Set key1 and key2 with the same value
            txn.set(&key1, &value).unwrap();
            txn.set(&key2, &value).unwrap();

            // Commit the transaction
            txn.commit().await.unwrap();
        }

        {
            // Start a new read-only transaction
            let txn = store.begin().unwrap();

            // Retrieve the value associated with key1
            let val = txn.get(&key1).unwrap().unwrap();

            // Assert that the value retrieved in txn matches the expected value
            assert_eq!(&val[..], value.as_ref());
        }

        {
            // Start a new read-only transaction
            let txn = store.begin().unwrap();

            // Retrieve the value associated with key2
            let val = txn.get(&key2).unwrap().unwrap();

            // Assert that the value retrieved in txn matches the expected value
            assert_eq!(val, value);
        }

        {
            // Start a new write transaction
            let mut txn = store.begin().unwrap();

            // Set key3 with the same value
            txn.set(&key3, &value).unwrap();

            // Commit the transaction
            txn.commit().await.unwrap();
        }

        {
            // Start a new read-only transaction
            let txn = store.begin().unwrap();

            // Retrieve the value associated with key3
            let val = txn.get(&key3).unwrap().unwrap();

            // Assert that the value retrieved in txn matches the expected value
            assert_eq!(val, value);
        }
    }

    #[tokio::test]
    async fn txn_with_value_read_from_memory() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 40;

        // Create a new Core instance with VariableKey as the key type
        let store = Store::new(opts).expect("should create store");

        // Define test keys and value
        let key1 = Bytes::from("foo1");
        let key2 = Bytes::from("foo2");
        let value = Bytes::from("bar");

        {
            // Start a new write transaction (txn)
            let mut txn = store.begin().unwrap();

            // Set key1 and key2 with the same value
            txn.set(&key1, &value).unwrap();
            txn.set(&key2, &value).unwrap();

            // Commit the transaction
            txn.commit().await.unwrap();
        }

        {
            // Start a new read-only transaction
            let txn = store.begin().unwrap();

            // Retrieve the value associated with key1
            let val = txn.get(&key1).unwrap().unwrap();

            // Assert that the value retrieved in txn matches the expected value
            assert_eq!(&val[..], value.as_ref());
        }

        {
            // Start a new read-only transaction
            let txn = store.begin().unwrap();

            // Retrieve the value associated with key2
            let val = txn.get(&key2).unwrap().unwrap();

            // Assert that the value retrieved in txn matches the expected value
            assert_eq!(val, value);
        }
    }
}
