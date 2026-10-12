//! The payload of a log record: a batch, as v0.20.0 to v0.21.4 encoded it. A frozen copy of the
//! read side of `batch.rs`.
//!
//! ```text
//! version (1 byte, 1)
//! starting sequence number (varint)
//! number of entries (varint)
//! entries, each: kind (1) | key length (varint) | key | value length (varint) | value | timestamp (varint)
//! pointers, one per entry: 0, or 1 and a value pointer of 25 bytes
//! ```
//!
//! An entry with a value length of 0 has no value. The pointers are the value pointer of the
//! entry's value, if the version that wrote the batch had already written the value to the value
//! log; v0.20.0 and v0.20.1 did, and logged the encoded pointer as the value of the entry too, so
//! they are consumed and checked here and not returned: the value of the entry is a value
//! location either way. v0.20.2 and later write no pointers.

use crate::{varint, Error, Kind, Result, WalBatch, WalEntry};

const BATCH_VERSION: u8 = 1;
const VALUE_POINTER_SIZE: usize = 25;

fn corrupt(what: &str) -> Error {
	Error::Corrupted(format!("a logged batch {what}"))
}

pub(crate) fn decode(data: &[u8]) -> Result<WalBatch> {
	let version = *data.first().ok_or_else(|| corrupt("is empty"))?;
	if version != BATCH_VERSION {
		return Err(Error::Corrupted(format!("a logged batch has the unknown version {version}")));
	}
	let mut pos = 1;
	let starting_seq =
		varint::read(data, &mut pos).ok_or_else(|| corrupt("has a bad sequence number"))?;
	let count = varint::read(data, &mut pos).ok_or_else(|| corrupt("has a bad entry count"))?;
	if count > u64::from(u32::MAX) {
		return Err(corrupt("has more than 2^32 entries"));
	}
	let count = count as usize;

	// Each entry takes at least 4 bytes, which keeps a damaged count from allocating.
	let mut entries = Vec::with_capacity(count.min(data.len() / 4));
	for _ in 0..count {
		let kind_byte = *data.get(pos).ok_or_else(|| corrupt("ends inside an entry"))?;
		pos += 1;
		let kind = Kind::from_byte(kind_byte).ok_or_else(|| {
			Error::Corrupted(format!(
				"a logged batch holds an entry of the unknown kind {kind_byte}"
			))
		})?;
		let key = take_sized(data, &mut pos, "key")?.to_vec();
		let value = take_sized(data, &mut pos, "value")?;
		let value = (!value.is_empty()).then(|| value.to_vec());
		let timestamp =
			varint::read(data, &mut pos).ok_or_else(|| corrupt("has a bad timestamp"))?;
		entries.push(WalEntry {
			kind,
			key,
			value,
			timestamp,
		});
	}

	// A batch that ends here has no pointers.
	if pos < data.len() {
		for _ in 0..count {
			let flag = *data.get(pos).ok_or_else(|| corrupt("ends inside its pointers"))?;
			pos += 1;
			match flag {
				0 => {}
				1 => {
					if data.len() - pos < VALUE_POINTER_SIZE {
						return Err(corrupt("ends inside a value pointer"));
					}
					pos += VALUE_POINTER_SIZE;
				}
				other => {
					return Err(Error::Corrupted(format!(
						"a logged batch has the pointer flag {other}"
					)))
				}
			}
		}
	}

	Ok(WalBatch {
		starting_seq,
		entries,
	})
}

/// A length (varint) and that many bytes.
fn take_sized<'a>(data: &'a [u8], pos: &mut usize, what: &str) -> Result<&'a [u8]> {
	let len =
		varint::read(data, pos).ok_or_else(|| corrupt(&format!("has a bad {what} length")))?;
	let end = usize::try_from(len)
		.ok()
		.and_then(|len| pos.checked_add(len))
		.filter(|end| *end <= data.len())
		.ok_or_else(|| corrupt(&format!("has a {what} that runs past its end")))?;
	let bytes = &data[*pos..end];
	*pos = end;
	Ok(bytes)
}

#[cfg(test)]
pub(crate) mod testing {
	use crate::block::testing::put_varint;

	/// An entry of a batch: kind, key, value and timestamp.
	pub(crate) type Item<'a> = (u8, &'a [u8], Option<&'a [u8]>, u64);

	/// A batch as v0.21.4 encoded it.
	pub(crate) fn encode(starting_seq: u64, entries: &[Item]) -> Vec<u8> {
		let mut out = vec![1];
		put_varint(&mut out, starting_seq);
		put_varint(&mut out, entries.len() as u64);
		for (kind, key, value, timestamp) in entries {
			out.push(*kind);
			put_varint(&mut out, key.len() as u64);
			out.extend_from_slice(key);
			let value = value.unwrap_or(&[]);
			put_varint(&mut out, value.len() as u64);
			out.extend_from_slice(value);
			put_varint(&mut out, *timestamp);
		}
		out.extend(std::iter::repeat_n(0u8, entries.len()));
		out
	}
}

#[cfg(test)]
mod tests {
	use super::testing::encode;
	use super::*;

	#[test]
	fn decodes_what_v0_21_encoded() {
		let bytes = encode(
			7,
			&[(2, b"a", Some(b"\x00\x01value"), 99), (0, b"b", None, 5), (1, b"c", None, u64::MAX)],
		);
		let batch = decode(&bytes).unwrap();
		assert_eq!(batch.starting_seq, 7);
		assert_eq!(batch.entries.len(), 3);
		assert_eq!(batch.entries[0].kind, Kind::Set);
		assert_eq!(batch.entries[0].key, b"a");
		assert_eq!(batch.entries[0].value.as_deref(), Some(&b"\x00\x01value"[..]));
		assert_eq!(batch.entries[0].timestamp, 99);
		assert_eq!(batch.entries[1].kind, Kind::Delete);
		assert_eq!(batch.entries[1].value, None);
		assert_eq!(batch.entries[2].kind, Kind::SoftDelete);
		assert_eq!(batch.entries[2].timestamp, u64::MAX);
	}

	#[test]
	fn consumes_the_pointers_of_v0_20() {
		let mut bytes = encode(1, &[(2, b"k", Some(b"\x01\x01pointer"), 0)]);
		bytes.pop();
		bytes.push(1);
		bytes.extend_from_slice(&[9; VALUE_POINTER_SIZE]);
		let batch = decode(&bytes).unwrap();
		assert_eq!(batch.entries[0].value.as_deref(), Some(&b"\x01\x01pointer"[..]));
		// A pointer that is cut short is not a batch.
		bytes.pop();
		assert!(decode(&bytes).is_err());
	}

	#[test]
	fn a_batch_that_ends_after_its_entries_has_no_pointers() {
		let mut bytes = encode(1, &[(2, b"k", Some(b"v"), 0)]);
		bytes.pop();
		assert_eq!(decode(&bytes).unwrap().entries.len(), 1);
	}

	#[test]
	fn refuses_what_is_not_a_batch_without_panicking() {
		let good = encode(3, &[(2, b"key", Some(b"value"), 1), (2, b"other", Some(b"v"), 2)]);
		for cut in 0..good.len() - 2 {
			assert!(decode(&good[..cut]).is_err(), "cut at {cut}");
		}
		assert!(decode(&[]).is_err());
		assert!(decode(&[2, 0, 0]).is_err());
		let mut bad_kind = good.clone();
		bad_kind[3] = 99;
		assert!(decode(&bad_kind).is_err());
		let huge = [1, 1, 0xff, 0xff, 0xff, 0xff, 0x0f];
		assert!(decode(&huge).is_err());
		for byte in 0..=255u8 {
			let mut flipped = good.clone();
			flipped[5] = byte;
			let _ = decode(&flipped);
		}
	}
}
