//! Block decompression and key-value entry parsing.

use std::io::Read;

use crate::error::{Error, Result};
use crate::varint::decode_varint_usize;

pub const BLOCK_TRAILER_LEN: usize = 5; // 1 byte compression + 4 bytes crc32

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
	None = 0,
	Snappy = 1,
	Zlib = 2,
	Lz4 = 3,
	Zstd = 4,
}

impl CompressionType {
	pub fn from_u8(val: u8) -> Result<Self> {
		match val {
			0 => Ok(Self::None),
			1 => Ok(Self::Snappy),
			2 => Ok(Self::Zlib),
			3 => Ok(Self::Lz4),
			4 => Ok(Self::Zstd),
			other => Err(Error::Decompression(format!("Unknown compression type {other}"))),
		}
	}
}

pub fn decompress_block(raw: &[u8]) -> Result<Vec<u8>> {
	if raw.len() < BLOCK_TRAILER_LEN {
		return Err(Error::CorruptBlock("Block shorter than trailer length".into()));
	}

	let data_len = raw.len() - BLOCK_TRAILER_LEN;
	let compressed = &raw[..data_len];
	let comp_type = CompressionType::from_u8(raw[data_len])?;
	let expected_crc = u32::from_le_bytes(raw[data_len + 1..data_len + 5].try_into().unwrap());

	let mut hasher = crc32fast::Hasher::new();
	hasher.update(compressed);
	hasher.update(&[raw[data_len]]);
	let calculated_crc = hasher.finalize();

	if expected_crc != 0 && calculated_crc != expected_crc {
		return Err(Error::ChecksumMismatch {
			expected: expected_crc,
			calculated: calculated_crc,
		});
	}

	match comp_type {
		CompressionType::None => Ok(compressed.to_vec()),
		CompressionType::Snappy => {
			let mut decoder = snap::read::FrameDecoder::new(compressed);
			let mut buf = Vec::new();
			decoder.read_to_end(&mut buf).map_err(|e| Error::Decompression(e.to_string()))?;
			Ok(buf)
		}
		CompressionType::Lz4 => lz4_flex::decompress_size_prepended(compressed)
			.or_else(|_| lz4_flex::decompress(compressed, 64 * 1024))
			.map_err(|e| Error::Decompression(e.to_string())),
		CompressionType::Zstd => {
			zstd::stream::decode_all(compressed).map_err(|e| Error::Decompression(e.to_string()))
		}
		CompressionType::Zlib => Err(Error::Decompression("Zlib compression not supported".into())),
	}
}

/// A parsed entry from a V1 block.
#[derive(Debug, Clone)]
pub struct RawEntry {
	pub full_key: Vec<u8>,
	pub value: Vec<u8>,
}

/// Iterates all entries inside a decompressed block.
pub fn parse_block_entries(block_data: &[u8]) -> Result<Vec<RawEntry>> {
	if block_data.len() < 4 {
		return Ok(Vec::new());
	}

	let num_restarts = u32::from_le_bytes(
		block_data[block_data.len() - 4..]
			.try_into()
			.map_err(|_| Error::CorruptBlock("Failed to read restart count".into()))?,
	) as usize;

	let restarts_len = num_restarts * 4;
	if block_data.len() < 4 + restarts_len {
		return Err(Error::CorruptBlock("Block buffer smaller than restarts array".into()));
	}

	let data_limit = block_data.len() - 4 - restarts_len;
	let mut cursor = 0;
	let mut last_key = Vec::new();
	let mut entries = Vec::new();

	while cursor < data_limit {
		let (shared, n1) = decode_varint_usize(&block_data[cursor..])
			.ok_or_else(|| Error::CorruptBlock("Failed to decode shared key len".into()))?;
		cursor += n1;

		let (unshared, n2) = decode_varint_usize(&block_data[cursor..])
			.ok_or_else(|| Error::CorruptBlock("Failed to decode unshared key len".into()))?;
		cursor += n2;

		let (vlen, n3) = decode_varint_usize(&block_data[cursor..])
			.ok_or_else(|| Error::CorruptBlock("Failed to decode value len".into()))?;
		cursor += n3;

		if cursor + unshared + vlen > data_limit {
			return Err(Error::CorruptBlock("Entry extends beyond data section".into()));
		}

		if shared > last_key.len() {
			return Err(Error::CorruptBlock("Shared prefix length exceeds previous key".into()));
		}

		let mut full_key = Vec::with_capacity(shared + unshared);
		full_key.extend_from_slice(&last_key[..shared]);
		full_key.extend_from_slice(&block_data[cursor..cursor + unshared]);
		cursor += unshared;

		let value = block_data[cursor..cursor + vlen].to_vec();
		cursor += vlen;

		last_key = full_key.clone();
		entries.push(RawEntry {
			full_key,
			value,
		});
	}

	Ok(entries)
}
