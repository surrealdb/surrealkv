//! Unsigned LEB128, which is what `integer-encoding` wrote and v0.x used for every variable
//! length integer.

/// Reads a varint at `*pos` and advances past it. `None` if the buffer ends inside it or it does
/// not fit in 64 bits.
pub(crate) fn read(buf: &[u8], pos: &mut usize) -> Option<u64> {
	let mut value: u64 = 0;
	let mut shift = 0u32;
	let mut i = *pos;
	loop {
		let byte = *buf.get(i)?;
		i += 1;
		if shift == 63 && byte > 1 {
			return None;
		}
		value |= u64::from(byte & 0x7f) << shift;
		if byte & 0x80 == 0 {
			*pos = i;
			return Some(value);
		}
		shift += 7;
		if shift > 63 {
			return None;
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn reads_what_leb128_writes() {
		let mut pos = 0;
		assert_eq!(read(&[0x00], &mut pos), Some(0));
		assert_eq!(pos, 1);
		let mut pos = 0;
		assert_eq!(read(&[0xac, 0x02, 0xff], &mut pos), Some(300));
		assert_eq!(pos, 2);
		let max = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
		let mut pos = 0;
		assert_eq!(read(&max, &mut pos), Some(u64::MAX));
		assert_eq!(pos, 10);
	}

	#[test]
	fn refuses_a_truncated_or_overlong_varint() {
		let mut pos = 0;
		assert_eq!(read(&[0x80], &mut pos), None);
		assert_eq!(pos, 0);
		let overlong = [0xff; 11];
		assert_eq!(read(&overlong, &mut pos), None);
		let too_big = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
		assert_eq!(read(&too_big, &mut pos), None);
	}
}
