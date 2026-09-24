//! Variable-length integer (varint) decoding utilities.

/// Decodes a 64-bit unsigned varint from `buf` starting at `offset`.
/// Returns `Some((value, bytes_read))` or `None` if buffer is too short or malformed.
#[inline]
pub fn decode_varint(buf: &[u8], offset: usize) -> Option<(u64, usize)> {
	let mut val = 0u64;
	let mut shift = 0;
	for (i, &b) in buf[offset..].iter().enumerate() {
		val |= ((b & 0x7f) as u64) << shift;
		if b & 0x80 == 0 {
			return Some((val, i + 1));
		}
		shift += 7;
		if shift >= 64 {
			return None;
		}
	}
	None
}

/// Decodes a 32-bit unsigned varint from `buf` starting at `offset`.
#[inline]
pub fn decode_varint32(buf: &[u8], offset: usize) -> Option<(u32, usize)> {
	let (val, len) = decode_varint(buf, offset)?;
	Some((val as u32, len))
}
