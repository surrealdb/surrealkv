//! Reading SSTable data blocks and extracting key-value entries.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::block::{decompress_block, parse_block_entries, BLOCK_TRAILER_LEN};
use crate::error::{Error, Result};
use crate::footer::{BlockHandle, Footer, V1_FOOTER_TOTAL_LEN};
use crate::key::V1ParsedKey;

#[derive(Debug, Clone)]
pub struct V1Record {
	pub user_key: Vec<u8>,
	pub value: Vec<u8>,
	pub timestamp: u64,
	pub seq_num: u64,
	pub is_tombstone: bool,
}

pub struct V1TableReader {
	file: File,
	footer: Footer,
}

impl V1TableReader {
	pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
		let mut file = File::open(path)?;
		let file_len = file.metadata()?.len() as usize;

		if file_len < V1_FOOTER_TOTAL_LEN {
			return Err(Error::CorruptFooter("File too small for footer".into()));
		}

		file.seek(SeekFrom::End(-(V1_FOOTER_TOTAL_LEN as i64)))?;
		let mut footer_buf = [0u8; V1_FOOTER_TOTAL_LEN];
		file.read_exact(&mut footer_buf)?;

		let footer = Footer::decode(&footer_buf)?;
		Ok(Self {
			file,
			footer,
		})
	}

	fn read_raw_block(&mut self, handle: &BlockHandle) -> Result<Vec<u8>> {
		let total_len = handle.size + BLOCK_TRAILER_LEN;
		let mut buf = vec![0u8; total_len];
		self.file.seek(SeekFrom::Start(handle.offset as u64))?;
		self.file.read_exact(&mut buf)?;
		decompress_block(&buf)
	}

	/// Reads all records from this SSTable, expanding all data blocks.
	pub fn read_all_records(&mut self) -> Result<Vec<V1Record>> {
		// Read top-level index block
		let index_handle = self.footer.index;
		let index_data = self.read_raw_block(&index_handle)?;
		let index_entries = parse_block_entries(&index_data)?;

		let mut data_handles = Vec::new();

		for entry in index_entries {
			if let Ok((handle, _)) = BlockHandle::decode(&entry.value) {
				// Check if this handle points to a partition index block or directly to a data
				// block
				data_handles.push(handle);
			}
		}

		let mut records = Vec::new();

		for handle in data_handles {
			if let Ok(block_data) = self.read_raw_block(&handle) {
				if let Ok(entries) = parse_block_entries(&block_data) {
					// Check if this is a secondary index block or data block
					let is_secondary_index =
						entries.iter().all(|e| BlockHandle::decode(&e.value).is_ok());

					if is_secondary_index && !entries.is_empty() {
						for sub_entry in entries {
							if let Ok((leaf_handle, _)) = BlockHandle::decode(&sub_entry.value) {
								if let Ok(leaf_data) = self.read_raw_block(&leaf_handle) {
									if let Ok(leaf_entries) = parse_block_entries(&leaf_data) {
										for item in leaf_entries {
											let parsed = V1ParsedKey::decode(&item.full_key);
											records.push(V1Record {
												user_key: parsed.user_key.to_vec(),
												value: item.value,
												timestamp: parsed.timestamp,
												seq_num: parsed.seq_num,
												is_tombstone: parsed.is_tombstone(),
											});
										}
									}
								}
							}
						}
					} else {
						for item in entries {
							let parsed = V1ParsedKey::decode(&item.full_key);
							records.push(V1Record {
								user_key: parsed.user_key.to_vec(),
								value: item.value,
								timestamp: parsed.timestamp,
								seq_num: parsed.seq_num,
								is_tombstone: parsed.is_tombstone(),
							});
						}
					}
				}
			}
		}

		Ok(records)
	}
}
