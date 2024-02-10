use hashbrown::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::storage::{
    kv::error::{Error, Result},
    kv::meta::Metadata,
    kv::store::Core,
    kv::util::{calculate_crc32, calculate_crc32_combined},
};

pub(crate) const MD_SIZE: usize = 1; // Size of txmdLen and kvmdLen in bytes
pub(crate) const MAX_KV_METADATA_SIZE: usize = 1; // Maximum size of key-value metadata in bytes
pub(crate) const MAX_TX_METADATA_SIZE: usize = 0; // Maximum size of transaction metadata in bytes
pub(crate) const TRANSACTION_HEADER_VERSION: u16 = 1; // Version of the transaction header

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
}

// Tx struct encoded format:
//
//   +---------------------------------------------------------+
//   |                     Tx (Transaction)                    |
//   |---------------------------------------------------------|
//   | header: TxHeader                                        |
//   | entries: Vec<TxEntry>                                   |
//   +---------------------------------------------------------+

//   +---------------------------------------------------------+
//   |                     TxHeader                            |
//   |---------------------------------------------------------|
//   | crc: u32                                                |
//   | id: u64                                                 |
//   | lsn: u64                                                |
//   | ts: u64                                                 |
//   | version: u16                                            |
//   | metadata: Option<Metadata>                              |
//   | num_entries: u16                                        |
//   +---------------------------------------------------------+

//   +---------------------------------------------------------+
//   |                        TxEntry                          |
//   |---------------------------------------------------------|
//   | key: Bytes                                              |
//   | key_len: u32                                            |
//   | md: Option<Metadata>                                    |
//   | value_len: u32                                          |
//   | value: Bytes                                            |
//   | crc32: u32                                              |
//   +---------------------------------------------------------+
//
//
// TxHeader encoded format:
//
//   |---------------------------------------------------------------------------------------------|------------------|
//   |                              TxHeader                                                       |     TxEntry[]    |
//   |--------|-------|--------|-------|------------|-----------------|-----------------|----------|------------------|
//   | crc(4) | id(8) | lsn(8) | ts(8) | version(2) | num_entries(2)  | metadata_len(2) | metadata |  ...entries...   |
//   |--------|-------|--------|-------|------------|-----------------|-----------------|----------|------------------|
//
// TxEntry encoded format:
//
//   |-----------------|----------|------------|-----|--------------|-------|-------|
//   | metadata_len(4) | metadata | key_len(4) | key | value_len(4) | value | crc32 |
//   |-----------------|----------|------------|-----|--------------|-------|-------|
//
#[derive(Debug)]
pub(crate) struct TxRecord {
    pub(crate) header: TxHeader,
    pub(crate) entries: Vec<TxEntry>,
}

impl TxRecord {
    pub(crate) fn new(max_entries: usize) -> Self {
        TxRecord {
            header: TxHeader::new(),
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
        let crc32 = calculate_crc32_combined(&entry.key, &entry.value);
        let tx_record_entry = TxEntry {
            key_len: entry.key.len() as u32,
            key: entry.key,
            metadata: entry.metadata,
            value_len: entry.value.len() as u32,
            value: entry.value,
            crc32,
        };
        self.entries.push(tx_record_entry);
        self.header.num_entries += 1;
    }

    pub(crate) fn encode(
        &self,
        buf: &mut BytesMut,
        current_offset: u64,
        offset_tracker: &mut HashMap<Bytes, usize>,
    ) -> Result<()> {
        // Encode header
        self.header.encode(buf);

        // Encode entries and store offsets
        for entry in &self.entries {
            let mut offset = entry.encode(buf)?;
            offset += current_offset as usize;

            // Store the offset for the current entry
            offset_tracker.insert(entry.key.clone(), offset);
        }

        let crc = calculate_crc32(buf);
        buf.put_u32(crc);

        Ok(())
    }

    pub(crate) fn to_buf(&self, buf: &mut BytesMut) -> Result<()> {
        // Encode header
        self.header.encode(buf);

        // Encode entries and store offsets
        for entry in &self.entries {
            entry.encode(buf)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct TxHeader {
    pub(crate) id: u64,
    pub(crate) lsn: u64,
    pub(crate) ts: u64,
    pub(crate) version: u16,
    pub(crate) num_entries: u32,
    pub(crate) metadata: Option<Metadata>,
}

impl TxHeader {
    pub(crate) fn new() -> Self {
        TxHeader {
            id: 0,
            lsn: 0,
            ts: 0,
            version: TRANSACTION_HEADER_VERSION,
            num_entries: 0,
            metadata: None,
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
                let md_bytes = metadata.to_bytes();
                let md_len = md_bytes.len() as u16;
                (md_len, md_bytes)
            }
            None => (0, Bytes::new()),
        };

        // tx_id(8) + lsn(8) + ts(8) + version(2) + num_entries(4) + meta_data_len(2) + metadata
        buf.put_u64(self.id);
        buf.put_u64(self.lsn);
        buf.put_u64(self.ts);
        buf.put_u16(self.version);
        buf.put_u32(self.num_entries);
        buf.put_u16(md_len);
        if md_len > 0 {
            buf.put(md_bytes);
        }
    }
}

#[derive(Debug)]
pub(crate) struct TxEntry {
    pub(crate) key: Bytes,
    pub(crate) key_len: u32,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) value_len: u32,
    pub(crate) value: Bytes,
    pub(crate) crc32: u32,
}

impl TxEntry {
    pub(crate) fn encode(&self, buf: &mut BytesMut) -> Result<usize> {
        // Encode metadata, if present
        if let Some(metadata) = &self.metadata {
            let md_bytes = metadata.to_bytes();
            let md_len = md_bytes.len() as u16;
            buf.put_u16(md_len);
            buf.put(md_bytes);
        } else {
            buf.put_u16(0);
        }

        // Encode key length, key, value length, value, and crc32
        buf.put_u32(self.key_len);
        buf.put(self.key.as_ref());
        buf.put_u32(self.value_len);
        let offset = buf.len();
        buf.put(self.value.as_ref());
        buf.put_u32(self.crc32);

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

        if self.flag == 1 {
            let value_bytes =
                cursor.get_ref()[cursor.position() as usize..][..self.value_length].as_ref();
            cursor.advance(self.value_length);

            self.value = Some(Bytes::copy_from_slice(value_bytes));
        } else {
            // Decode version, value length, and value offset
            self.value_offset = Some(cursor.get_u64());
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
        let vlog = self.store.clog.read();
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

        let mut value_ref = ValueRef::new(store.core.clone());
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
