//! RocksDB BlockHandle representation.

use crate::error::{Error, Result};
use crate::varint::decode_varint;

/// A BlockHandle references a block within an SSTable by offset and size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BlockHandle {
	pub offset: u64,
	pub size: u64,
}

impl BlockHandle {
	pub const fn new(offset: u64, size: u64) -> Self {
		Self {
			offset,
			size,
		}
	}

	/// Decodes a BlockHandle from a slice starting at `offset`.
	/// Returns the decoded BlockHandle and the total number of bytes read.
	pub fn decode(buf: &[u8], offset: usize) -> Result<(Self, usize)> {
		let (block_offset, n1) = decode_varint(buf, offset).ok_or(Error::CorruptBlock {
			offset: offset as u64,
			reason: "Failed to decode BlockHandle offset varint".to_string(),
		})?;

		let (block_size, n2) =
			decode_varint(buf, offset + n1).ok_or(Error::CorruptBlock {
				offset: (offset + n1) as u64,
				reason: "Failed to decode BlockHandle size varint".to_string(),
			})?;

		Ok((Self::new(block_offset, block_size), n1 + n2))
	}
}
