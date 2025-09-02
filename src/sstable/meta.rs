use std::cmp::{max, min};
use std::mem::size_of;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    error::Error,
    sstable::{table::TableFormat, InternalKey},
    CompressionType, Result, Value,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyRange {
    pub(crate) low: Value,
    pub(crate) high: Value,
}

impl KeyRange {
    pub fn new(range: (Value, Value)) -> Self {
        KeyRange {
            low: range.0,
            high: range.1,
        }
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.high >= other.low && self.low <= other.high
    }

    /// Creates a new KeyRange that encompasses both input ranges.
    /// This is useful for finding the total span of multiple ranges.
    pub fn merge(&self, other: &Self) -> Self {
        KeyRange {
            low: min(self.low.clone(), other.low.clone()),
            high: max(self.high.clone(), other.high.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Properties {
    pub(crate) id: u64,
    pub(crate) table_format: TableFormat,
    pub(crate) num_entries: u64,
    pub(crate) num_deletions: u64,
    pub(crate) data_size: u64,
    pub(crate) global_seq_num: u64,
    pub(crate) num_data_blocks: u64,
    pub(crate) top_level_index_size: u64,
    pub(crate) created_at: u128,
    pub(crate) item_count: u64,
    pub(crate) key_count: u64,
    pub(crate) tombstone_count: u64,
    pub(crate) file_size: u64,
    pub(crate) block_size: u32,
    pub(crate) block_count: u32,
    pub(crate) compression: CompressionType,
    pub(crate) seqnos: (u64, u64),
    pub(crate) key_range: Option<KeyRange>,
}

impl Properties {
    pub fn new() -> Self {
        Properties {
            id: 0,
            table_format: TableFormat::LSMV1,
            num_entries: 0,
            num_deletions: 0,
            data_size: 0,
            global_seq_num: 0,
            num_data_blocks: 0,
            top_level_index_size: 0,
            created_at: 0,
            item_count: 0,
            key_count: 0,
            tombstone_count: 0,
            file_size: 0,
            block_size: 0,
            block_count: 0,
            compression: CompressionType::None,
            seqnos: (0, 0),
            key_range: None,
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(128);
        buf.put_u64(self.id);
        buf.put_u8(self.table_format as u8);
        buf.put_u64(self.num_entries);
        buf.put_u64(self.num_deletions);
        buf.put_u64(self.data_size);
        buf.put_u64(self.global_seq_num);
        buf.put_u64(self.num_data_blocks);
        buf.put_u64(self.top_level_index_size);
        buf.put_u128(self.created_at);
        buf.put_u64(self.item_count);
        buf.put_u64(self.key_count);
        buf.put_u64(self.tombstone_count);
        buf.put_u64(self.file_size);
        buf.put_u32(self.block_size);
        buf.put_u32(self.block_count);
        buf.put_u8(self.compression as u8);
        buf.put_u64(self.seqnos.0);
        buf.put_u64(self.seqnos.1);
        if let Some(ref key_range) = self.key_range {
            // Write the size of the first element and then the element itself
            let size_first_element = key_range.low.len() as u64;
            buf.put_u64(size_first_element); // Write the size
            buf.extend_from_slice(&key_range.low); // Write the element

            // Write the size of the second element and then the element itself
            let size_second_element = key_range.high.len() as u64;
            buf.put_u64(size_second_element); // Write the size
            buf.extend_from_slice(&key_range.high); // Write the element
        }
        buf.freeze()
    }

    pub fn decode(mut buf: Bytes) -> Result<Self> {
        let id = buf.get_u64();
        let table_format = buf.get_u8();
        let num_entries = buf.get_u64();
        let num_deletions = buf.get_u64();
        let data_size = buf.get_u64();
        let global_seq_num = buf.get_u64();
        let num_data_blocks = buf.get_u64();
        let top_level_index_size = buf.get_u64();
        let created_at = buf.get_u128();
        let item_count = buf.get_u64();
        let key_count = buf.get_u64();
        let tombstone_count = buf.get_u64();
        let file_size = buf.get_u64();
        let block_size = buf.get_u32();
        let block_count = buf.get_u32();
        let compression = buf.get_u8();
        let seqno_start = buf.get_u64();
        let seqno_end = buf.get_u64();
        let key_range = if buf.has_remaining() {
            let size_first_element = buf.get_u64() as usize;
            let first_element = buf.copy_to_bytes(size_first_element);
            let size_second_element = buf.get_u64() as usize;
            let second_element = buf.copy_to_bytes(size_second_element);
            let range = (
                first_element.as_ref().into(),
                second_element.as_ref().into(),
            );
            Some(KeyRange::new(range))
        } else {
            None
        };

        Ok(Self {
            id,
            table_format: TableFormat::from_u8(table_format)?,
            num_entries,
            num_deletions,
            data_size,
            global_seq_num,
            num_data_blocks,
            top_level_index_size,
            created_at,
            item_count,
            key_count,
            tombstone_count,
            file_size,
            block_size,
            block_count,
            compression: CompressionType::from(compression),
            seqnos: (seqno_start, seqno_end),
            key_range,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TableMetadata {
    pub(crate) has_point_keys: Option<bool>,
    pub(crate) smallest_seq_num: u64,
    pub(crate) largest_seq_num: u64,
    pub(crate) properties: Properties,
    pub(crate) smallest_point: Option<InternalKey>,
    pub(crate) largest_point: Option<InternalKey>,
}

impl TableMetadata {
    pub fn new() -> Self {
        TableMetadata {
            smallest_point: None,
            largest_point: None,
            has_point_keys: None,
            smallest_seq_num: 0,
            largest_seq_num: 0,
            properties: Properties::new(),
        }
    }

    pub fn set_smallest_point_key(&mut self, k: InternalKey) {
        self.smallest_point = Some(k);
        self.has_point_keys = Some(true);
    }

    pub fn set_largest_point_key(&mut self, k: InternalKey) {
        self.largest_point = Some(k);
        self.has_point_keys = Some(true);
    }

    pub fn update_seq_num(&mut self, seq_num: u64) {
        // Handle first sequence number specially
        if self.largest_seq_num == 0 && self.smallest_seq_num == 0 {
            self.smallest_seq_num = seq_num;
            self.largest_seq_num = seq_num;
        } else {
            if self.smallest_seq_num > seq_num {
                self.smallest_seq_num = seq_num;
            }
            if self.largest_seq_num < seq_num {
                self.largest_seq_num = seq_num;
            }
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Encode has_point_keys as 0 for None, 1 for Some(true), and 2 for Some(false)
        match self.has_point_keys {
            None => buf.put_u8(0),
            Some(true) => buf.put_u8(1),
            Some(false) => buf.put_u8(2),
        }

        // Encode smallest_seq_num and largest_seq_num as u64
        buf.put_u64(self.smallest_seq_num);
        buf.put_u64(self.largest_seq_num);

        let properties_encoded = self.properties.encode();
        let properties_encoded_len = properties_encoded.len() as u64;
        buf.put_u64(properties_encoded_len);
        buf.put(properties_encoded);

        // Encode smallest_point and largest_point
        match &self.smallest_point {
            None => buf.put_u8(0),
            Some(key) => {
                buf.put_u8(1);
                let key_encoded = key.encode();
                buf.put_u64(key_encoded.len() as u64); // Write the size of the encoded key
                buf.extend_from_slice(&key_encoded); // Write the encoded key itself
            }
        }

        match &self.largest_point {
            None => buf.put_u8(0),
            Some(key) => {
                buf.put_u8(1);
                let key_encoded = key.encode();
                buf.put_u64(key_encoded.len() as u64); // Write the size of the encoded key
                buf.extend_from_slice(&key_encoded); // Write the encoded key itself
            }
        }

        buf.freeze()
    }

    pub fn decode(src: &Bytes) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(src);

        // Decode has_point_keys
        let has_point_keys = match cursor.get_u8() {
            0 => None,
            1 => Some(true),
            2 => Some(false),
            _ => {
                return Err(Error::CorruptedTableMetadata(
                    "Invalid has_point_keys value".into(),
                ))
            }
        };

        // Decode smallest_seq_num and largest_seq_num
        let smallest_seq_num = cursor.get_u64();
        let largest_seq_num = cursor.get_u64();

        // Decode properties
        let properties_len = cursor.get_u64() as usize;
        let mut properties_bytes = vec![0u8; properties_len];
        cursor.copy_to_slice(&mut properties_bytes);
        let properties_bytes = Bytes::from(properties_bytes);
        let properties = Properties::decode(properties_bytes)?;

        // Decode smallest_point
        let smallest_point = match cursor.get_u8() {
            0 => None,
            1 => {
                let key_len = cursor.get_u64() as usize;
                let mut key_bytes = vec![0u8; key_len];
                cursor.copy_to_slice(&mut key_bytes);
                let key_bytes = Bytes::from(key_bytes);
                Some(InternalKey::decode(&key_bytes))
            }
            _ => {
                return Err(Error::CorruptedTableMetadata(
                    "Invalid smallest_point value".into(),
                ))
            }
        };

        // Decode largest_point
        let largest_point = match cursor.get_u8() {
            0 => None,
            1 => {
                let key_len = cursor.get_u64() as usize;
                let mut key_bytes = vec![0u8; key_len];
                cursor.copy_to_slice(&mut key_bytes);
                let key_bytes = Bytes::from(key_bytes);
                Some(InternalKey::decode(&key_bytes))
            }
            _ => {
                return Err(Error::CorruptedTableMetadata(
                    "Invalid largest_point value".into(),
                ))
            }
        };

        Ok(TableMetadata {
            has_point_keys,
            smallest_seq_num,
            largest_seq_num,
            properties,
            smallest_point,
            largest_point,
        })
    }
}

pub(crate) fn size_of_writer_metadata() -> usize {
    size_of::<Option<bool>>()
        + size_of::<u64>() * 2
        + size_of_properties()
        + size_of::<Option<InternalKey>>() * 2
}

fn size_of_properties() -> usize {
    size_of::<u64>() * 11
        + size_of::<TableFormat>()
        + size_of::<u128>()
        + size_of::<u32>() * 2
        + size_of::<CompressionType>()
        + size_of::<(u64, u64)>()
        + size_of::<Option<KeyRange>>()
}
