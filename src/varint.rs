// ============================================================================
// Internal varint (LEB128) encoding and decoding utilities
// ============================================================================

/// Encodes a u64 integer into a variable number of bytes using standard unsigned LEB128.
/// Returns the number of bytes written to `dst`.
#[inline]
pub(crate) fn encode_varint_u64(mut val: u64, dst: &mut [u8]) -> usize {
	let mut i = 0;
	while val >= 0x80 {
		dst[i] = (val as u8) | 0x80;
		val >>= 7;
		i += 1;
	}
	dst[i] = val as u8;
	i + 1
}

/// Appends a u64 as a variable-length LEB128 integer into `buf`.
#[inline]
pub(crate) fn put_varint_u64(buf: &mut Vec<u8>, mut val: u64) {
	while val >= 0x80 {
		buf.push((val as u8) | 0x80);
		val >>= 7;
	}
	buf.push(val as u8);
}

/// Appends a u32 as a variable-length LEB128 integer into `buf`.
#[inline]
pub(crate) fn put_varint_u32(buf: &mut Vec<u8>, val: u32) {
	put_varint_u64(buf, val as u64);
}

/// Decodes an unsigned LEB128 integer from `src`.
/// Returns `Some((value, bytes_read))` or `None` if the input is truncated or malformed.
#[inline]
pub(crate) fn decode_varint_u64(src: &[u8]) -> Option<(u64, usize)> {
	let mut val = 0u64;
	let mut shift = 0;
	for (i, &b) in src.iter().enumerate() {
		if shift >= 64 {
			return None;
		}
		val |= ((b & 0x7F) as u64) << shift;
		if (b & 0x80) == 0 {
			return Some((val, i + 1));
		}
		shift += 7;
	}
	None
}

/// Decodes a u32 from variable-length LEB128 integer in `src`.
#[inline]
pub(crate) fn decode_varint_u32(src: &[u8]) -> Option<(u32, usize)> {
	let (v, n) = decode_varint_u64(src)?;
	if v > u32::MAX as u64 {
		return None;
	}
	Some((v as u32, n))
}

/// Decodes a usize from variable-length LEB128 integer in `src`.
#[inline]
pub(crate) fn decode_varint_usize(src: &[u8]) -> Option<(usize, usize)> {
	let (v, n) = decode_varint_u64(src)?;
	Some((v as usize, n))
}

/// Calculates required space to encode a u64 in varint format.
#[inline]
pub(crate) const fn varint_len_u64(mut val: u64) -> usize {
	let mut len = 1;
	while val >= 0x80 {
		val >>= 7;
		len += 1;
	}
	len
}
