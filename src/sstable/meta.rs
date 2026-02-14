use bytes::{Buf, BufMut, BytesMut};

use crate::error::Error;
use crate::sstable::error::SSTableError;
use crate::sstable::table::TableFormat;
use crate::{CompressionType, InternalKey, Result};

#[derive(Debug, Clone)]
pub(crate) struct Properties {
	pub(crate) id: u64,
	pub(crate) table_format: TableFormat,
	pub(crate) num_entries: u64,
	pub(crate) num_deletions: u64,
	pub(crate) data_size: u64,
	pub(crate) oldest_vlog_file_id: u64,
	pub(crate) num_data_blocks: u64,

	// Index metrics
	pub(crate) index_size: u64,
	pub(crate) index_partitions: u64,
	pub(crate) top_level_index_size: u64,

	// Filter metrics
	pub(crate) filter_size: u64,

	// Raw size metrics (uncompressed)
	pub(crate) raw_key_size: u64,
	pub(crate) raw_value_size: u64,

	pub(crate) created_at: u128,
	pub(crate) item_count: u64,
	pub(crate) key_count: u64,
	pub(crate) tombstone_count: u64,
	pub(crate) num_soft_deletes: u64,

	// Range deletion metrics
	pub(crate) num_range_deletions: u64,

	pub(crate) block_size: u32,
	pub(crate) block_count: u32,
	pub(crate) compression: CompressionType,
	pub(crate) seqnos: (u64, u64),

	// Time metrics
	pub(crate) oldest_key_time: Option<u64>,
	pub(crate) newest_key_time: Option<u64>,
}

impl Properties {
	pub(crate) fn new() -> Self {
		Properties {
			id: 0,
			table_format: TableFormat::LSMV1,
			num_entries: 0,
			num_deletions: 0,
			data_size: 0,
			oldest_vlog_file_id: 0,
			num_data_blocks: 0,
			index_size: 0,
			index_partitions: 0,
			top_level_index_size: 0,
			filter_size: 0,
			raw_key_size: 0,
			raw_value_size: 0,
			created_at: 0,
			item_count: 0,
			key_count: 0,
			tombstone_count: 0,
			num_soft_deletes: 0,
			num_range_deletions: 0,
			block_size: 0,
			block_count: 0,
			compression: CompressionType::None,
			seqnos: (0, 0),
			oldest_key_time: None,
			newest_key_time: None,
		}
	}

	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut buf = BytesMut::with_capacity(256);
		buf.put_u64(self.id);
		buf.put_u8(self.table_format as u8);
		buf.put_u64(self.num_entries);
		buf.put_u64(self.num_deletions);
		buf.put_u64(self.data_size);
		buf.put_u64(self.oldest_vlog_file_id);
		buf.put_u64(self.num_data_blocks);
		buf.put_u64(self.index_size);
		buf.put_u64(self.index_partitions);
		buf.put_u64(self.top_level_index_size);
		buf.put_u64(self.filter_size);
		buf.put_u64(self.raw_key_size);
		buf.put_u64(self.raw_value_size);
		buf.put_u128(self.created_at);
		buf.put_u64(self.item_count);
		buf.put_u64(self.key_count);
		buf.put_u64(self.tombstone_count);
		buf.put_u64(self.num_soft_deletes);
		buf.put_u64(self.num_range_deletions);
		buf.put_u32(self.block_size);
		buf.put_u32(self.block_count);
		buf.put_u8(self.compression as u8);
		buf.put_u64(self.seqnos.0);
		buf.put_u64(self.seqnos.1);
		buf.put_u64(self.oldest_key_time.unwrap_or(0));
		buf.put_u64(self.newest_key_time.unwrap_or(0));
		buf.to_vec()
	}

	pub(crate) fn decode(buf: Vec<u8>) -> Result<Self> {
		let mut buf = &buf[..];
		let id = buf.get_u64();
		let table_format = buf.get_u8();
		let num_entries = buf.get_u64();
		let num_deletions = buf.get_u64();
		let data_size = buf.get_u64();
		let oldest_vlog_file_id = buf.get_u64();
		let num_data_blocks = buf.get_u64();
		let index_size = buf.get_u64();
		let index_partitions = buf.get_u64();
		let top_level_index_size = buf.get_u64();
		let filter_size = buf.get_u64();
		let raw_key_size = buf.get_u64();
		let raw_value_size = buf.get_u64();
		let created_at = buf.get_u128();
		let item_count = buf.get_u64();
		let key_count = buf.get_u64();
		let tombstone_count = buf.get_u64();
		let num_soft_deletes = buf.get_u64();
		let num_range_deletions = buf.get_u64();
		let block_size = buf.get_u32();
		let block_count = buf.get_u32();
		let compression = buf.get_u8();
		let seqno_start = buf.get_u64();
		let seqno_end = buf.get_u64();
		let oldest_key_time = Some(buf.get_u64());
		let newest_key_time = Some(buf.get_u64());

		Ok(Self {
			id,
			table_format: TableFormat::from_u8(table_format)?,
			num_entries,
			num_deletions,
			data_size,
			oldest_vlog_file_id,
			num_data_blocks,
			index_size,
			index_partitions,
			top_level_index_size,
			filter_size,
			raw_key_size,
			raw_value_size,
			created_at,
			item_count,
			key_count,
			tombstone_count,
			num_soft_deletes,
			num_range_deletions,
			block_size,
			block_count,
			compression: CompressionType::try_from(compression)?,
			seqnos: (seqno_start, seqno_end),
			oldest_key_time,
			newest_key_time,
		})
	}
}

#[derive(Debug, Clone)]
pub struct TableMetadata {
	pub(crate) has_point_keys: Option<bool>,
	pub(crate) smallest_seq_num: Option<u64>,
	pub(crate) largest_seq_num: Option<u64>,
	pub(crate) properties: Properties,
	pub(crate) smallest_point: Option<InternalKey>,
	pub(crate) largest_point: Option<InternalKey>,
}

impl TableMetadata {
	pub(crate) fn new() -> Self {
		TableMetadata {
			smallest_point: None,
			largest_point: None,
			has_point_keys: None,
			smallest_seq_num: None,
			largest_seq_num: None,
			properties: Properties::new(),
		}
	}

	pub(crate) fn set_smallest_point_key(&mut self, k: InternalKey) {
		self.smallest_point = Some(k);
		self.has_point_keys = Some(true);
	}

	pub(crate) fn set_largest_point_key(&mut self, k: InternalKey) {
		self.largest_point = Some(k);
		self.has_point_keys = Some(true);
	}

	pub(crate) fn update_seq_num(&mut self, seq_num: u64) {
		self.smallest_seq_num = Some(self.smallest_seq_num.map_or(seq_num, |s| s.min(seq_num)));
		self.largest_seq_num = Some(self.largest_seq_num.map_or(seq_num, |l| l.max(seq_num)));
	}

	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut buf = BytesMut::new();

		// Encode has_point_keys as 0 for None, 1 for Some(true), and 2 for Some(false)
		match self.has_point_keys {
			None => buf.put_u8(0),
			Some(true) => buf.put_u8(1),
			Some(false) => buf.put_u8(2),
		}

		// Encode smallest_seq_num and largest_seq_num as u64
		// Write 0 if not set (only happens for empty tables which aren't persisted)
		buf.put_u64(self.smallest_seq_num.unwrap_or(0));
		buf.put_u64(self.largest_seq_num.unwrap_or(0));

		let properties_encoded = self.properties.encode();
		let properties_encoded_len = properties_encoded.len() as u64;
		buf.put_u64(properties_encoded_len);
		buf.extend_from_slice(&properties_encoded);

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

		buf.to_vec()
	}

	pub(crate) fn decode(src: &[u8]) -> Result<TableMetadata> {
		let mut cursor = std::io::Cursor::new(src);

		// Decode has_point_keys
		let has_point_keys = match cursor.get_u8() {
			0 => None,
			1 => Some(true),
			2 => Some(false),
			value => {
				return Err(Error::from(SSTableError::InvalidHasPointKeysValue {
					value,
				}))
			}
		};

		// Decode smallest_seq_num and largest_seq_num
		// Always Some since persisted tables have entries
		let smallest_seq_num = Some(cursor.get_u64());
		let largest_seq_num = Some(cursor.get_u64());

		// Decode properties
		let properties_len = cursor.get_u64() as usize;
		let mut properties_bytes = vec![0u8; properties_len];
		cursor.copy_to_slice(&mut properties_bytes);
		let properties = Properties::decode(properties_bytes)?;

		// Decode smallest_point
		let smallest_point = match cursor.get_u8() {
			0 => None,
			1 => {
				let key_len: usize = cursor.get_u64() as usize;
				let mut key_bytes = vec![0u8; key_len];
				cursor.copy_to_slice(&mut key_bytes);
				Some(InternalKey::decode(&key_bytes))
			}
			value => {
				return Err(Error::from(SSTableError::InvalidSmallestPointValue {
					value,
				}))
			}
		};

		// Decode largest_point
		let largest_point = match cursor.get_u8() {
			0 => None,
			1 => {
				let key_len = cursor.get_u64() as usize;
				let mut key_bytes = vec![0u8; key_len];
				cursor.copy_to_slice(&mut key_bytes);
				Some(InternalKey::decode(&key_bytes))
			}
			value => {
				return Err(Error::from(SSTableError::InvalidLargestPointValue {
					value,
				}))
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
