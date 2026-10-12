//! Block checksums, as RocksDB's table/format.cc computes them.

use xxhash_rust::xxh3::xxh3_64;
use xxhash_rust::xxh32::Xxh32;
use xxhash_rust::xxh64::Xxh64;

use crate::coding::{crc32c, crc32c_extend, mask_crc};
use crate::error::{unsupported, Result};

/// The checksum type recorded in a table footer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumType {
	None,
	Crc32c,
	XxHash,
	XxHash64,
	Xxh3,
}

impl ChecksumType {
	pub(crate) fn from_u8(value: u8) -> Result<Self> {
		match value {
			0 => Ok(Self::None),
			1 => Ok(Self::Crc32c),
			2 => Ok(Self::XxHash),
			3 => Ok(Self::XxHash64),
			4 => Ok(Self::Xxh3),
			other => Err(unsupported(format!("checksum type {other}"))),
		}
	}
}

/// `ModifyChecksumForLastByte`: XXH3 folds the last byte in with a multiply, not a second pass.
fn modify_for_last_byte(checksum: u32, last_byte: u8) -> u32 {
	const RANDOM_PRIME: u32 = 0x6b90_83d9;
	checksum ^ u32::from(last_byte).wrapping_mul(RANDOM_PRIME)
}

/// `ComputeBuiltinChecksumWithLastByte`: the checksum of `data` followed by `last_byte`, the
/// compression type that sits in the block trailer.
pub(crate) fn with_last_byte(kind: ChecksumType, data: &[u8], last_byte: u8) -> u32 {
	match kind {
		ChecksumType::None => 0,
		ChecksumType::Crc32c => mask_crc(crc32c_extend(crc32c(data), &[last_byte])),
		ChecksumType::XxHash => {
			let mut state = Xxh32::new(0);
			state.update(data);
			state.update(&[last_byte]);
			state.digest()
		}
		ChecksumType::XxHash64 => {
			let mut state = Xxh64::new(0);
			state.update(data);
			state.update(&[last_byte]);
			state.digest() as u32
		}
		ChecksumType::Xxh3 => modify_for_last_byte(xxh3_64(data) as u32, last_byte),
	}
}

/// `ChecksumModifierForContext`: the value that format versions 6 and above add to the checksum of
/// the block at `offset`, so that a block moved to another place in the file no longer verifies.
pub(crate) fn context_modifier(base_context_checksum: u32, offset: u64) -> u32 {
	let all_or_nothing = 0u32.wrapping_sub(u32::from(base_context_checksum != 0));
	let modifier = base_context_checksum ^ ((offset as u32).wrapping_add((offset >> 32) as u32));
	modifier & all_or_nothing
}
