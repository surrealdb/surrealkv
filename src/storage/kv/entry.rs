use ahash::HashMap;
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher as crc32Hasher;

use crate::storage::{
    kv::error::{Error, Result},
    kv::meta::Metadata,
    kv::store::Core,
};

pub(crate) const MD_SIZE: usize = 1; // Size of txmdLen and kvmdLen in bytes
pub(crate) const MAX_KV_METADATA_SIZE: usize = 2; // Maximum size of key-value metadata in bytes
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

    pub(crate) fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = Some(metadata);
    }

    pub(crate) fn set_ts(&mut self, ts: u64) {
        self.ts = ts;
    }

    pub(crate) fn mark_delete(&mut self) {
        if self.metadata.is_none() {
            self.metadata = Some(Metadata::new());
        }
        self.metadata.as_mut().unwrap().as_deleted(true).unwrap();
    }

    pub(crate) fn mark_tombstone(&mut self) {
        if self.metadata.is_none() {
            self.metadata = Some(Metadata::new());
        }
        self.metadata.as_mut().unwrap().as_tombstone(true).unwrap();
    }

    pub(crate) fn is_deleted_or_tombstone(&self) -> bool {
        if let Some(metadata) = &self.metadata {
            metadata.is_deleted_or_tombstone()
        } else {
            false
        }
    }
}

pub(crate) fn encode_entries(
    entries: &[Entry],
    tx_id: u64,
    buf: &mut BytesMut,
    offset_tracker: &mut HashMap<Bytes, u64>,
) {
    for entry in entries {
        let tx_record_entry = Record::new_from_entry(entry.clone(), tx_id);
        let offset = tx_record_entry.encode(buf).unwrap();
        offset_tracker.insert(entry.key.clone(), offset as u64);
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

impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.ts.hash(state);
        self.version.hash(state);
        self.key_len.hash(state);
        self.key.hash(state);
        self.metadata.hash(state);
        self.value_len.hash(state);
        self.value.hash(state);
    }
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

    pub(crate) fn calculate_crc32(&self) -> u32 {
        let mut hasher = crc32Hasher::new();
        self.hash(&mut hasher);
        hasher.finalize()
    }

    pub(crate) fn new_from_entry(entry: Entry, tx_id: u64) -> Self {
        let rec = Record {
            id: tx_id,
            ts: entry.ts,
            crc32: 0, // Temporarily set to 0, will be updated after initialization
            version: RECORD_VERSION,
            key_len: entry.key.len() as u32,
            key: entry.key,
            metadata: entry.metadata,
            value_len: entry.value.len() as u32,
            value: entry.value,
        };

        let crc32 = rec.calculate_crc32();

        // Return a new Record instance with the correct crc32 value
        Record { crc32, ..rec }
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

    pub(crate) fn from_entry(entry: Entry, tx_id: u64) -> Record {
        let rec = Record {
            id: tx_id,
            ts: entry.ts,
            crc32: 0, // Temporarily set to 0, will be updated after initialization
            version: RECORD_VERSION,
            key_len: entry.key.len() as u32,
            key: entry.key,
            metadata: entry.metadata,
            value_len: entry.value.len() as u32,
            value: entry.value,
        };

        let crc32 = rec.calculate_crc32();

        // Return a new Record instance with the correct crc32 value
        Record { crc32, ..rec }
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

pub trait Value {
    fn resolve(&self) -> Result<Vec<u8>>;
    fn ts(&self) -> u64;
    fn metadata(&self) -> Option<&Metadata>;
    fn length(&self) -> usize;
    fn segment_id(&self) -> u64;
}

/// Value reference implementation.
pub struct ValueRef {
    pub(crate) flag: u8,
    pub(crate) ts: u64,
    pub(crate) segment_id: u64,
    pub(crate) value_length: usize,
    pub(crate) value_offset: Option<u64>,
    pub(crate) value: Option<Bytes>,
    pub(crate) metadata: Option<Metadata>,
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

    fn metadata(&self) -> Option<&Metadata> {
        self.metadata.as_ref()
    }

    fn length(&self) -> usize {
        self.value_length
    }

    fn segment_id(&self) -> u64 {
        self.segment_id
    }
}

impl ValueRef {
    pub(crate) fn new(store: Arc<Core>) -> Self {
        ValueRef {
            ts: 0,
            flag: 0,
            segment_id: 0,
            value_offset: None,
            value_length: 0,
            value: None,
            metadata: None,
            store,
        }
    }

    /// Encode the valueRef into a byte representation.
    pub(crate) fn encode(
        segment_id: u64,
        value: &Bytes,
        metadata: Option<&Metadata>,
        value_offset: u64,
        max_value_threshold: usize,
    ) -> Bytes {
        let mut buf = BytesMut::new();

        // Memory layout:
        // | swizzle flag (1 byte) | segment_id (8 bytes) | value length (4 bytes) |
        // | value_offset (8 bytes) | value (variable length, if inlined) |
        // | metadata length (2 bytes) | metadata (variable length) |

        if value.len() <= max_value_threshold {
            // Inline value case
            buf.put_u8(1); // swizzle flag to indicate value is inlined or stored in log
            buf.put_u64(segment_id); // Segment ID
            buf.put_u32(value.len() as u32); // Length of the value
            buf.put_u64(value_offset); // Offset where the value is stored
            buf.put(value.as_ref()); // The value itself
        } else {
            // Reference value case
            buf.put_u8(0); // swizzle flag to indicate value is not inlined
            buf.put_u64(segment_id); // Segment ID
            buf.put_u32(value.len() as u32); // Length of the value
            buf.put_u64(value_offset); // Offset where the value is stored
                                       // Note: The actual value is not included in the buffer for this case
        }

        if let Some(metadata) = &metadata {
            // If metadata is present
            let md_bytes = metadata.to_bytes();
            let md_len = md_bytes.len() as u16;
            buf.put_u16(md_len); // Put the length of metadata
            buf.put(md_bytes); // Put the metadata bytes
        } else {
            // If no metadata is present, just write a length of 0
            buf.put_u16(0); // Metadata length indicator for no metadata
        }
        buf.freeze() // Converts the buffer into `Bytes` without copying
    }

    /// Encode the valueRef into an in-memory byte representation.
    pub(crate) fn encode_mem(value: &Bytes, metadata: Option<&Metadata>) -> Bytes {
        let mut buf = BytesMut::new();

        // Memory layout for encoding a value with optional metadata into memory:
        // | swizzle flag (1 byte) | reserved (8 bytes) | value length (4 bytes) |
        // | reserved (8 bytes) | value (variable length) |
        // | metadata length (2 bytes) | metadata (variable length) |

        buf.put_u8(1); // swizzle flag to indicate value is inlined or stored in log
        buf.put_u64(0); // Reserved 8 bytes, currently set to 0
        buf.put_u32(value.len() as u32); // Length of the value
        buf.put_u64(0); // Another reserved 8 bytes, currently set to 0
        buf.put(value.as_ref()); // The actual value bytes

        if let Some(metadata) = &metadata {
            // If metadata is provided
            let md_bytes = metadata.to_bytes();
            let md_len = md_bytes.len() as u16;
            buf.put_u16(md_len); // Write the length of metadata
            buf.put(md_bytes); // Write the metadata bytes themselves
        } else {
            // If no metadata is provided, indicate this with a length of 0
            buf.put_u16(0); // Metadata length set to 0 to indicate absence
        }
        buf.freeze() // Converts the buffer into `Bytes` without copying, ready for use
    }

    /// Decode the byte representation into a valueRef.
    pub(crate) fn decode(&mut self, ts: u64, encoded_bytes: &Bytes) -> Result<()> {
        let mut cursor = Cursor::new(encoded_bytes);

        // Set the timestamp for the entry
        self.ts = ts;

        // Read the flag to determine if the value is inlined or referenced externally
        self.flag = cursor.get_u8();

        // Decode the segment ID, value length, and value offset from the encoded bytes
        self.segment_id = cursor.get_u64();
        self.value_length = cursor.get_u32() as usize;
        self.value_offset = Some(cursor.get_u64());

        // If the flag indicates the value is inlined, extract it directly from the encoded bytes
        if self.flag == 1 {
            // Calculate the start and end positions of the value within the encoded bytes
            let value_bytes =
                cursor.get_ref()[cursor.position() as usize..][..self.value_length].to_vec();
            // Advance the cursor past the value to continue decoding any remaining data
            cursor.advance(self.value_length);

            // Store the extracted value
            self.value = Some(Bytes::from(value_bytes));
        } else {
            // For non-inlined values, the value offset is already set, and the actual value is not in the encoded bytes
        }

        // Decode key-value metadata, if present
        // Check if there's enough data left for metadata size; if not, the data is corrupted
        if encoded_bytes.len() < cursor.position() as usize + MD_SIZE {
            return Err(Error::CorruptedIndex);
        }

        // Read the length of the key-value metadata
        let kv_metadata_len = cursor.get_u16() as usize;
        if kv_metadata_len > 0 {
            // Validate the metadata size to prevent processing invalid or malicious data
            if kv_metadata_len > MAX_KV_METADATA_SIZE {
                return Err(Error::CorruptedIndex);
            }
            // Ensure there's enough data left for the actual metadata; if not, the data is corrupted
            if encoded_bytes.len() < cursor.position() as usize + kv_metadata_len {
                return Err(Error::CorruptedIndex);
            }
            // Extract the metadata bytes and advance the cursor
            let kv_metadata_bytes =
                cursor.get_ref()[cursor.position() as usize..][..kv_metadata_len].to_vec();
            cursor.advance(kv_metadata_len);

            // Convert the raw metadata bytes into a Metadata object and store it
            self.metadata = Some(Metadata::from_bytes(&kv_metadata_bytes)?);
        } else {
            // If there's no metadata, set the corresponding field to None
            self.metadata = None;
        }

        // After processing all expected data, ensure the cursor is at the end of the encoded bytes
        // If not, it indicates the encoded data may be corrupted or improperly formatted
        if cursor.position() as usize != encoded_bytes.len() {
            return Err(Error::CorruptedIndex);
        }

        Ok(())
    }

    /// Resolves the value from the given offset in the commit log.
    /// If the offset exists in the value cache, it returns the cached value.
    /// Otherwise, it reads the value from the commit log, caches it, and returns it.
    fn resolve_from_offset(&self, value_offset: u64) -> Result<Vec<u8>> {
        // Attempt to return the cached value if it exists
        let cache_key = (self.segment_id, value_offset);

        if let Some(value) = self.store.value_cache.get(&cache_key) {
            return Ok(value.to_vec());
        }

        // If the value is not in the cache, read it from the commit log
        let mut buf = vec![0; self.value_length];
        let clog = self.store.clog.as_ref().unwrap().read();
        clog.read_at(&mut buf, self.segment_id, value_offset)?;

        // Cache the newly read value for future use
        self.store
            .value_cache
            .insert(cache_key, Bytes::from(buf.clone()));

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
        value_ref.metadata = Some(kvmd);

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
        //     decoded_value_ref.metadata.unwrap().deleted(),
        //     value_ref.metadata.unwrap().deleted()
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
            let mut txn = store.begin().unwrap();

            // Retrieve the value associated with key1
            let val = txn.get(&key1).unwrap().unwrap();

            // Assert that the value retrieved in txn matches the expected value
            assert_eq!(&val[..], value.as_ref());
        }

        {
            // Start a new read-only transaction
            let mut txn = store.begin().unwrap();

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
            let mut txn = store.begin().unwrap();

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
            let mut txn = store.begin().unwrap();

            // Retrieve the value associated with key1
            let val = txn.get(&key1).unwrap().unwrap();

            // Assert that the value retrieved in txn matches the expected value
            assert_eq!(&val[..], value.as_ref());
        }

        {
            // Start a new read-only transaction
            let mut txn = store.begin().unwrap();

            // Retrieve the value associated with key2
            let val = txn.get(&key2).unwrap().unwrap();

            // Assert that the value retrieved in txn matches the expected value
            assert_eq!(val, value);
        }
    }
}
