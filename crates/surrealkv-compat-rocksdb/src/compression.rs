//! Block and blob decompression, in the framing RocksDB writes (compress_format_version 2).

use crate::coding::varint64;
use crate::error::{corrupt, unsupported, Result};

pub(crate) const NONE: u8 = 0;
pub(crate) const SNAPPY: u8 = 1;
pub(crate) const ZLIB: u8 = 2;
pub(crate) const BZIP2: u8 = 3;
pub(crate) const LZ4: u8 = 4;
pub(crate) const LZ4HC: u8 = 5;
pub(crate) const XPRESS: u8 = 6;
pub(crate) const ZSTD: u8 = 7;

/// Decompresses `data`, which was compressed with `kind`.
///
/// Snappy writes its own length; the others are preceded by the uncompressed size as a varint.
pub(crate) fn decompress(kind: u8, data: &[u8]) -> Result<Vec<u8>> {
	match kind {
		NONE => Ok(data.to_vec()),
		SNAPPY => snap::raw::Decoder::new()
			.decompress_vec(data)
			.map_err(|e| corrupt(format!("a Snappy block does not decompress: {e}"))),
		LZ4 | LZ4HC => {
			let mut pos = 0;
			let size = varint64(data, &mut pos)? as usize;
			lz4_flex::block::decompress(&data[pos..], size)
				.map_err(|e| corrupt(format!("an LZ4 block does not decompress: {e}")))
		}
		ZSTD => {
			let mut pos = 0;
			let size = varint64(data, &mut pos)? as usize;
			zstd::bulk::decompress(&data[pos..], size)
				.map_err(|e| corrupt(format!("a Zstandard block does not decompress: {e}")))
		}
		ZLIB => Err(unsupported("Zlib-compressed blocks")),
		BZIP2 => Err(unsupported("BZip2-compressed blocks")),
		XPRESS => Err(unsupported("Xpress-compressed blocks")),
		other => Err(unsupported(format!("compression type {other}"))),
	}
}
