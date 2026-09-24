//! SurrealKV V1 SSTable footer representation.

use crate::error::{Error, Result};
use crate::varint::decode_varint_usize;

pub const V1_FOOTER_TOTAL_LEN: usize = 50;
pub const V1_MAGIC_FOOTER: [u8; 8] = [0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
	pub offset: usize,
	pub size: usize,
}

impl BlockHandle {
	pub fn decode(src: &[u8]) -> Result<(Self, usize)> {
		let (offset, n1) = decode_varint_usize(src)
			.ok_or_else(|| Error::CorruptBlock("Failed to decode block offset".into()))?;
		let (size, n2) = decode_varint_usize(&src[n1..])
			.ok_or_else(|| Error::CorruptBlock("Failed to decode block size".into()))?;
		Ok((
			Self {
				offset,
				size,
			},
			n1 + n2,
		))
	}
}

#[derive(Debug, Clone)]
pub struct Footer {
	pub format: u8,
	pub checksum: u8,
	pub meta_index: BlockHandle,
	pub index: BlockHandle,
}

impl Footer {
	pub fn decode(buf: &[u8]) -> Result<Self> {
		if buf.len() < V1_FOOTER_TOTAL_LEN {
			return Err(Error::CorruptFooter(format!(
				"Buffer too small: expected at least {V1_FOOTER_TOTAL_LEN}, got {}",
				buf.len()
			)));
		}

		let footer_bytes = &buf[buf.len() - V1_FOOTER_TOTAL_LEN..];
		let magic = &footer_bytes[footer_bytes.len() - V1_MAGIC_FOOTER.len()..];
		if magic != V1_MAGIC_FOOTER {
			return Err(Error::BadMagic {
				expected: V1_MAGIC_FOOTER.to_vec(),
				found: magic.to_vec(),
			});
		}

		let format = footer_bytes[0];
		let checksum = footer_bytes[1];

		let (meta_index, n1) = BlockHandle::decode(&footer_bytes[2..])?;
		let (index, _) = BlockHandle::decode(&footer_bytes[2 + n1..])?;

		Ok(Self {
			format,
			checksum,
			meta_index,
			index,
		})
	}
}
