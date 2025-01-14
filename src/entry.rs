use ahash::HashMap;
use std::hash::{Hash, Hasher};

use bytes::{BufMut, Bytes, BytesMut};
use crc32fast::Hasher as crc32Hasher;

use crate::{error::Result, meta::Metadata};

pub(crate) const MAX_KV_METADATA_SIZE: usize = 2; // Maximum size of key-value metadata in bytes
pub(crate) const RECORD_VERSION: u16 = 1; // Version of the transaction header

#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) key: Bytes,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) value: Bytes,
    pub(crate) ts: u64,
    pub(crate) replace: bool,
}

impl Entry {
    pub(crate) fn new(key: &[u8], value: &[u8]) -> Self {
        Entry {
            key: Bytes::copy_from_slice(key),
            metadata: None,
            value: Bytes::copy_from_slice(value),
            ts: 0,
            replace: false,
        }
    }

    pub(crate) fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = Some(metadata);
    }

    pub(crate) fn set_ts(&mut self, ts: u64) {
        self.ts = ts;
    }

    pub(crate) fn set_replace(&mut self, replace: bool) {
        self.replace = replace;
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

#[derive(Debug, Clone)]
pub struct Record {
    pub id: u64,
    pub ts: u64,
    pub version: u16,
    pub metadata: Option<Metadata>,
    pub key: Bytes,
    pub key_len: u32,
    pub value_len: u32,
    pub value: Bytes,
    pub crc32: u32,
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

impl Default for Record {
    fn default() -> Self {
        Self::new()
    }
}

impl Record {
    pub fn new() -> Self {
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

    pub fn reset(&mut self) {
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

    pub fn encode(&self, buf: &mut BytesMut) -> Result<usize> {
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::option::Options;
    use crate::store::Store;

    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[test]
    fn txn_with_value_read_from_clog() {
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
            txn.commit().unwrap();
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
            txn.commit().unwrap();
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

    #[test]
    fn txn_with_value_read_from_memory() {
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
            txn.commit().unwrap();
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
