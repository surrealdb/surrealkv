//! Varint encoding and decoding utilities.

#[inline]
pub fn decode_varint_u64(src: &[u8]) -> Option<(u64, usize)> {
	let mut val: u64 = 0;
	let mut shift: u32 = 0;

	for (i, &b) in src.iter().enumerate() {
		if shift >= 64 {
			return None;
		}
		val |= ((b & 0x7f) as u64) << shift;
		if (b & 0x80) == 0 {
			return Some((val, i + 1));
		}
		shift += 7;
	}

	None
}

#[inline]
pub fn decode_varint_usize(src: &[u8]) -> Option<(usize, usize)> {
	decode_varint_u64(src).map(|(v, n)| (v as usize, n))
}
