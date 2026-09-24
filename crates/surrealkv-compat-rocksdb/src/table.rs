//! Single RocksDB SSTable reader in pure Rust.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::block::{Block, decompress_block};
use crate::error::{Error, Result};
use crate::footer::Footer;
use crate::handle::BlockHandle;

/// A decoded key-value record from a RocksDB SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RocksDbEntry {
	/// The user key (with timestamp suffix stripped if versioned).
	pub key: Vec<u8>,
	/// The stored value payload.
	pub value: Vec<u8>,
	/// RocksDB sequence number for conflict and ordering resolution.
	pub seq_num: u64,
	/// Whether this record represents a deletion tombstone.
	pub is_tombstone: bool,
	/// Extracted SurrealDB user-defined timestamp if present.
	pub timestamp: Option<u64>,
}

/// Reader for a single RocksDB BlockBasedTable (.sst) file.
pub struct SstReader {
	file: File,
	data_handles: Vec<BlockHandle>,
	has_user_timestamps: bool,
}

impl SstReader {
	/// Opens and parses an SSTable at `path`.
	///
	/// If `has_user_timestamps` is true, the trailing 8 bytes of the user key
	/// are treated as an 8-byte little-endian timestamp (SurrealDB UDT format).
	pub fn open<P: AsRef<Path>>(path: P, has_user_timestamps: bool) -> Result<Self> {
		let mut file = File::open(path)?;
		let file_size = file.metadata()?.len();

		let footer = Footer::read_from(&mut file, file_size)?;

		// Read index block
		let index_handle = footer.index_handle;
		let mut index_raw = vec![0u8; (index_handle.size + 5) as usize];
		file.seek(SeekFrom::Start(index_handle.offset))?;
		file.read_exact(&mut index_raw)?;

		let comp_type = index_raw[index_handle.size as usize];
		let decompressed = decompress_block(&index_raw[..index_handle.size as usize], comp_type)?;
		let index_block = Block::new(decompressed)?;

		let mut data_handles = Vec::new();
		for entry in index_block.iter() {
			let (_idx_key, val) = entry?;
			let (handle, _) = BlockHandle::decode(&val, 0)?;
			data_handles.push(handle);
		}

		Ok(Self {
			file,
			data_handles,
			has_user_timestamps,
		})
	}

	/// Returns an iterator over all records in this SSTable in ascending order.
	pub fn iter(&mut self) -> SstIter<'_> {
		SstIter {
			reader: self,
			handle_idx: 0,
			current_block_iter: None,
		}
	}
}

/// Iterator over key-value records across all data blocks in an SSTable.
pub struct SstIter<'a> {
	reader: &'a mut SstReader,
	handle_idx: usize,
	current_block_iter: Option<crate::block::BlockIter>,
}

impl Iterator for SstIter<'_> {
	type Item = Result<RocksDbEntry>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			if let Some(ref mut iter) = self.current_block_iter {
				if let Some(entry_res) = iter.next() {
					match entry_res {
						Ok((raw_key, val)) => {
							if raw_key.len() < 8 {
								return Some(Err(Error::CorruptBlock {
									offset: 0,
									reason: "Key smaller than 8-byte RocksDB trailer".to_string(),
								}));
							}

							let trailer_offset = raw_key.len() - 8;
							let trailer = u64::from_le_bytes(
								raw_key[trailer_offset..].try_into().unwrap(),
							);
							let seq_num = trailer >> 8;
							let val_type = (trailer & 0xff) as u8;
							let is_tombstone = val_type == 0; // 0 = kTypeDeletion

							let mut user_key = &raw_key[..trailer_offset];
							let mut timestamp = None;

							if self.reader.has_user_timestamps && user_key.len() >= 8 {
								let ts_offset = user_key.len() - 8;
								let ts = u64::from_le_bytes(
									user_key[ts_offset..].try_into().unwrap(),
								);
								timestamp = Some(ts);
								user_key = &user_key[..ts_offset];
							}

							return Some(Ok(RocksDbEntry {
								key: user_key.to_vec(),
								value: val,
								seq_num,
								is_tombstone,
								timestamp,
							}));
						}
						Err(e) => return Some(Err(e)),
					}
				}
			}

			// Advance to next data block
			if self.handle_idx >= self.reader.data_handles.len() {
				return None;
			}

			let handle = self.reader.data_handles[self.handle_idx];
			self.handle_idx += 1;

			let mut raw_bytes = vec![0u8; (handle.size + 5) as usize];
			if let Err(e) = self.reader.file.seek(SeekFrom::Start(handle.offset)) {
				return Some(Err(Error::Io(e)));
			}
			if let Err(e) = self.reader.file.read_exact(&mut raw_bytes) {
				return Some(Err(Error::Io(e)));
			}

			let comp_type = raw_bytes[handle.size as usize];
			match decompress_block(&raw_bytes[..handle.size as usize], comp_type) {
				Ok(decompressed) => match Block::new(decompressed) {
					Ok(block) => {
						self.current_block_iter = Some(block.iter());
					}
					Err(e) => return Some(Err(e)),
				},
				Err(e) => return Some(Err(e)),
			}
		}
	}
}
