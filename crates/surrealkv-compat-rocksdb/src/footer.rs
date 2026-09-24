//! RocksDB BlockBasedTable Footer parser.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use crate::block::{decompress_block, Block};
use crate::error::{Error, Result};
use crate::handle::BlockHandle;

/// Modern RocksDB BlockBasedTable magic number (little-endian u64).
pub const ROCKSDB_MAGIC: u64 = 0x88e241b785f4cff7;

/// Legacy LevelDB/RocksDB BlockBasedTable magic number.
pub const ROCKSDB_LEGACY_MAGIC: u64 = 0xdb4775248b80fb57;

/// Max encoded footer size in modern RocksDB format versions.
pub const FOOTER_ENCODED_LEN_V2_PLUS: usize = 53;

/// Legacy format footer size.
pub const FOOTER_ENCODED_LEN_LEGACY: usize = 48;

/// Parsed BlockBasedTable Footer.
#[derive(Debug, Clone)]
pub struct Footer {
	pub format_version: u32,
	pub index_handle: BlockHandle,
	pub metaindex_handle: Option<BlockHandle>,
}

impl Footer {
	/// Decodes a Footer from an open SSTable file.
	pub fn read_from(file: &mut File, file_size: u64) -> Result<Self> {
		if file_size < FOOTER_ENCODED_LEN_V2_PLUS as u64 {
			return Err(Error::CorruptFooter {
				offset: file_size,
			});
		}

		let mut footer_buf = [0u8; FOOTER_ENCODED_LEN_V2_PLUS];
		file.seek(SeekFrom::End(-(FOOTER_ENCODED_LEN_V2_PLUS as i64)))?;
		file.read_exact(&mut footer_buf)?;

		let magic = u64::from_le_bytes(footer_buf[45..53].try_into().unwrap());
		if magic != ROCKSDB_MAGIC && magic != ROCKSDB_LEGACY_MAGIC {
			return Err(Error::InvalidMagic(footer_buf[45..53].try_into().unwrap()));
		}

		let format_version = u32::from_le_bytes(footer_buf[41..45].try_into().unwrap());

		if format_version >= 6 {
			// RocksDB format_version >= 6 (Modern RocksDB 8/9/10/11 default)
			// Metaindex size is encoded at offset 13..17
			let metaindex_size = u32::from_le_bytes(footer_buf[13..17].try_into().unwrap()) as u64;
			let footer_offset = file_size - FOOTER_ENCODED_LEN_V2_PLUS as u64;
			let metaindex_end = footer_offset - 5; // 5 bytes block trailer (1 comp byte + 4 crc)
			let metaindex_offset = metaindex_end.saturating_sub(metaindex_size);
			let metaindex_handle = BlockHandle::new(metaindex_offset, metaindex_size);

			// Read and decode the metaindex block to find "rocksdb.index"
			let mut meta_block_raw = vec![0u8; (metaindex_size + 5) as usize];
			file.seek(SeekFrom::Start(metaindex_offset))?;
			file.read_exact(&mut meta_block_raw)?;

			let comp_type = meta_block_raw[metaindex_size as usize];
			let decompressed =
				decompress_block(&meta_block_raw[..metaindex_size as usize], comp_type)?;
			let meta_block = Block::new(decompressed)?;

			let mut index_handle = None;
			for entry in meta_block.iter() {
				let (key, val) = entry?;
				if key == b"rocksdb.index" {
					let (handle, _) = BlockHandle::decode(&val, 0)?;
					index_handle = Some(handle);
					break;
				}
			}

			let index_handle = index_handle.ok_or_else(|| Error::CorruptBlock {
				offset: metaindex_offset,
				reason: "Metaindex missing rocksdb.index block handle".to_string(),
			})?;

			Ok(Self {
				format_version,
				index_handle,
				metaindex_handle: Some(metaindex_handle),
			})
		} else {
			// RocksDB format_version < 6 (standard varint handles in footer)
			let (metaindex_handle, n1) = BlockHandle::decode(&footer_buf, 0)?;
			let (index_handle, _) = BlockHandle::decode(&footer_buf, n1)?;

			Ok(Self {
				format_version,
				index_handle,
				metaindex_handle: Some(metaindex_handle),
			})
		}
	}
}
