//! RocksDB Block and BlockIter implementation.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::varint::decode_varint;

/// Decompresses raw block data according to RocksDB compression type.
pub fn decompress_block(compressed: &[u8], comp_type: u8) -> Result<Vec<u8>> {
	match comp_type {
		0 => Ok(compressed.to_vec()), // kNoCompression
		1 => {
			// kSnappyCompression
			let mut decoder = snap::raw::Decoder::new();
			decoder
				.decompress_vec(compressed)
				.map_err(|e| Error::DecompressionFailed(format!("Snappy: {e}")))
		}
		4 => {
			// kLZ4Compression / kLZ4HCCompression
			lz4_flex::decompress_size_prepended(compressed)
				.or_else(|_| lz4_flex::decompress(compressed, compressed.len() * 4))
				.map_err(|e| Error::DecompressionFailed(format!("LZ4: {e}")))
		}
		7 => {
			// kZSTD
			zstd::decode_all(compressed)
				.map_err(|e| Error::DecompressionFailed(format!("Zstandard: {e}")))
		}
		other => Err(Error::UnsupportedCompression(other)),
	}
}

/// A decoded in-memory block from a RocksDB BlockBasedTable.
pub struct Block {
	data: Arc<Vec<u8>>,
	data_len: usize,
}

impl Block {
	/// Decodes a block from raw uncompressed (or already decompressed) bytes.
	pub fn new(data: Vec<u8>) -> Result<Self> {
		if data.len() < 4 {
			return Ok(Self {
				data: Arc::new(data),
				data_len: 0,
			});
		}

		let n = data.len();
		let num_restarts = u32::from_le_bytes(data[n - 4..].try_into().unwrap()) as usize;
		let restarts_size = num_restarts * 4;
		if n < 4 + restarts_size {
			return Err(Error::CorruptBlock {
				offset: 0,
				reason: format!("Restart array length {restarts_size} exceeds block size {n}"),
			});
		}

		let restarts_start = n - 4 - restarts_size;

		Ok(Self {
			data: Arc::new(data),
			data_len: restarts_start,
		})
	}

	/// Returns an iterator over all entries in this block.
	pub fn iter(&self) -> BlockIter {
		BlockIter {
			data: Arc::clone(&self.data),
			data_len: self.data_len,
			offset: 0,
			current_key: Vec::new(),
		}
	}
}

/// Sequential iterator over prefix-compressed key-value entries in a block.
pub struct BlockIter {
	data: Arc<Vec<u8>>,
	data_len: usize,
	offset: usize,
	current_key: Vec<u8>,
}

impl Iterator for BlockIter {
	type Item = Result<(Vec<u8>, Vec<u8>)>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.offset >= self.data_len {
			return None;
		}

		let (shared, n1) = match decode_varint(&self.data, self.offset) {
			Some(v) => v,
			None => {
				return Some(Err(Error::CorruptBlock {
					offset: self.offset as u64,
					reason: "Failed to decode shared key length".to_string(),
				}))
			}
		};
		self.offset += n1;

		let (unshared, n2) = match decode_varint(&self.data, self.offset) {
			Some(v) => v,
			None => {
				return Some(Err(Error::CorruptBlock {
					offset: self.offset as u64,
					reason: "Failed to decode unshared key length".to_string(),
				}))
			}
		};
		self.offset += n2;

		let (val_len, n3) = match decode_varint(&self.data, self.offset) {
			Some(v) => v,
			None => {
				return Some(Err(Error::CorruptBlock {
					offset: self.offset as u64,
					reason: "Failed to decode value length".to_string(),
				}))
			}
		};
		self.offset += n3;

		let shared = shared as usize;
		let unshared = unshared as usize;
		let val_len = val_len as usize;

		if self.offset + unshared + val_len > self.data_len {
			return Some(Err(Error::CorruptBlock {
				offset: self.offset as u64,
				reason: "Entry spans past end of block data".to_string(),
			}));
		}

		self.current_key.truncate(shared);
		self.current_key.extend_from_slice(&self.data[self.offset..self.offset + unshared]);
		self.offset += unshared;

		let val = self.data[self.offset..self.offset + val_len].to_vec();
		self.offset += val_len;

		Some(Ok((self.current_key.clone(), val)))
	}
}
