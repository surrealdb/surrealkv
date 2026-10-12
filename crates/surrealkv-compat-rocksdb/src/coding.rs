//! The primitive encodings of RocksDB: varints, fixed-width little-endian integers, length-prefixed
//! slices, and CRC32C.

use std::sync::OnceLock;

use crate::error::{corrupt, Result};

pub(crate) fn varint64(buf: &[u8], pos: &mut usize) -> Result<u64> {
	let mut result = 0u64;
	let mut shift = 0u32;
	loop {
		let byte = *buf.get(*pos).ok_or_else(|| corrupt("a varint runs past the end"))?;
		*pos += 1;
		result |= u64::from(byte & 0x7f) << shift;
		if byte & 0x80 == 0 {
			return Ok(result);
		}
		shift += 7;
		if shift > 63 {
			return Err(corrupt("a varint is longer than 10 bytes"));
		}
	}
}

pub(crate) fn varint32(buf: &[u8], pos: &mut usize) -> Result<u32> {
	u32::try_from(varint64(buf, pos)?).map_err(|_| corrupt("a varint32 does not fit 32 bits"))
}

/// A signed value as RocksDB's `PutVarsignedint64` writes it: the sign in the lowest bit.
pub(crate) fn varsigned64(buf: &[u8], pos: &mut usize) -> Result<i64> {
	let raw = varint64(buf, pos)?;
	let magnitude = (raw >> 1) as i64;
	Ok(if raw & 1 == 0 {
		magnitude
	} else {
		-magnitude - 1
	})
}

pub(crate) fn fixed32(buf: &[u8], pos: &mut usize) -> Result<u32> {
	let bytes = slice(buf, pos, 4)?;
	Ok(u32::from_le_bytes(bytes.try_into().expect("4 bytes")))
}

pub(crate) fn fixed64(buf: &[u8], pos: &mut usize) -> Result<u64> {
	let bytes = slice(buf, pos, 8)?;
	Ok(u64::from_le_bytes(bytes.try_into().expect("8 bytes")))
}

pub(crate) fn slice<'a>(buf: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8]> {
	let end = pos.checked_add(len).filter(|end| *end <= buf.len());
	let end = end.ok_or_else(|| corrupt("a field runs past the end of its record"))?;
	let out = &buf[*pos..end];
	*pos = end;
	Ok(out)
}

pub(crate) fn length_prefixed<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
	let len = varint32(buf, pos)? as usize;
	slice(buf, pos, len)
}

// ---------- CRC32C (Castagnoli), as RocksDB's util/crc32c.cc ----------

const POLY: u32 = 0x82F6_3B78;

fn tables() -> &'static [[u32; 256]; 8] {
	static TABLES: OnceLock<[[u32; 256]; 8]> = OnceLock::new();
	TABLES.get_or_init(|| {
		let mut t = [[0u32; 256]; 8];
		for i in 0..256u32 {
			let mut c = i;
			for _ in 0..8 {
				c = if c & 1 == 1 {
					(c >> 1) ^ POLY
				} else {
					c >> 1
				};
			}
			t[0][i as usize] = c;
		}
		for i in 0..256usize {
			let mut c = t[0][i];
			for k in 1..8 {
				c = t[0][(c & 0xff) as usize] ^ (c >> 8);
				t[k][i] = c;
			}
		}
		t
	})
}

/// `crc32c::Extend`: continues `crc` over `data`.
pub(crate) fn crc32c_extend(crc: u32, data: &[u8]) -> u32 {
	let t = tables();
	let mut c = crc ^ 0xffff_ffff;
	let mut pos = 0;
	while pos + 8 <= data.len() {
		let lo = u32::from_le_bytes(data[pos..pos + 4].try_into().expect("4")) ^ c;
		let hi = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().expect("4"));
		c = t[7][(lo & 0xff) as usize]
			^ t[6][((lo >> 8) & 0xff) as usize]
			^ t[5][((lo >> 16) & 0xff) as usize]
			^ t[4][(lo >> 24) as usize]
			^ t[3][(hi & 0xff) as usize]
			^ t[2][((hi >> 8) & 0xff) as usize]
			^ t[1][((hi >> 16) & 0xff) as usize]
			^ t[0][(hi >> 24) as usize];
		pos += 8;
	}
	for byte in &data[pos..] {
		c = t[0][((c ^ u32::from(*byte)) & 0xff) as usize] ^ (c >> 8);
	}
	c ^ 0xffff_ffff
}

pub(crate) fn crc32c(data: &[u8]) -> u32 {
	crc32c_extend(0, data)
}

const MASK_DELTA: u32 = 0xa282_ead8;

/// `crc32c::Mask`: the form of a CRC that is stored in files.
pub(crate) fn mask_crc(crc: u32) -> u32 {
	crc.rotate_right(15).wrapping_add(MASK_DELTA)
}

pub(crate) fn unmask_crc(masked: u32) -> u32 {
	let rot = masked.wrapping_sub(MASK_DELTA);
	rot.rotate_left(15)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn crc32c_known_answers() {
		// Test vectors from RFC 3720 (iSCSI), the same ones RocksDB's own tests use.
		assert_eq!(crc32c(&[0u8; 32]), 0x8a91_36aa);
		assert_eq!(crc32c(&[0xffu8; 32]), 0x62a8_ab43);
		let ascending: Vec<u8> = (0..32).collect();
		assert_eq!(crc32c(&ascending), 0x46dd_794e);
		let descending: Vec<u8> = (0..32).rev().collect();
		assert_eq!(crc32c(&descending), 0x113f_db5c);
		assert_eq!(crc32c(b"123456789"), 0xe306_9283);
	}

	#[test]
	fn extend_continues() {
		let whole = crc32c(b"hello world");
		let split = crc32c_extend(crc32c(b"hello "), b"world");
		assert_eq!(whole, split);
	}

	#[test]
	fn mask_round_trips() {
		for v in [0u32, 1, 0xdead_beef, u32::MAX] {
			assert_eq!(unmask_crc(mask_crc(v)), v);
		}
	}

	#[test]
	fn varints() {
		let mut pos = 0;
		assert_eq!(varint64(&[0x96, 0x01], &mut pos).unwrap(), 150);
		assert_eq!(pos, 2);
		let mut pos = 0;
		assert!(varint64(&[0x80], &mut pos).is_err());
		// signed: 0 -> 0, 1 -> -1 (sign in the lowest bit), 2 -> 1
		let mut pos = 0;
		assert_eq!(varsigned64(&[0x00], &mut pos).unwrap(), 0);
		let mut pos = 0;
		assert_eq!(varsigned64(&[0x01], &mut pos).unwrap(), -1);
		let mut pos = 0;
		assert_eq!(varsigned64(&[0x02], &mut pos).unwrap(), 1);
	}
}
