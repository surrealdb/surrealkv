//! The value of an entry: a frozen copy of v0.x's `ValueLocation` (`vlog.rs`).
//!
//! ```text
//! meta (1 byte; bit 0 set: the bytes are a value pointer) | version (1 byte, 1) | bytes
//! ```
//!
//! A value pointer is 25 bytes: version (1), file id (u32), offset (u64), key size (u32), value
//! size (u32) and checksum (u32), all big endian. This crate does not read the value log.

use crate::{Error, Kind, Result, Value};

const BIT_VALUE_POINTER: u8 = 1;
const VALUE_POINTER_SIZE: usize = 25;

pub(crate) fn decode_value(kind: Kind, stored: &[u8]) -> Result<Option<Value>> {
	match kind {
		Kind::Delete => return Ok(None),
		Kind::RangeDelete => {
			return Err(Error::Unsupported(
				"the value of a range delete is the end of its range, not a value location".into(),
			))
		}
		// v0.x wrote a soft delete with no value at all.
		Kind::SoftDelete if stored.is_empty() => return Ok(None),
		_ => {}
	}
	let [meta, _version, payload @ ..] = stored else {
		return Err(Error::Corrupted(format!(
			"the value of an entry of kind {kind:?} is {} bytes, not a value location",
			stored.len()
		)));
	};
	let pointer = meta & BIT_VALUE_POINTER != 0;
	if pointer && payload.len() != VALUE_POINTER_SIZE {
		return Err(Error::Corrupted(format!(
			"a value pointer is {} bytes, not {VALUE_POINTER_SIZE}",
			payload.len()
		)));
	}
	Ok(Some(Value {
		pointer,
		payload: payload.to_vec(),
	}))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn decodes_inline_values_and_pointers() {
		let value = decode_value(Kind::Set, &[0, 1, b'a', b'b']).unwrap().unwrap();
		assert!(!value.pointer);
		assert_eq!(value.payload, b"ab");
		let empty = decode_value(Kind::Replace, &[0, 1]).unwrap().unwrap();
		assert!(!empty.pointer);
		assert!(empty.payload.is_empty());

		let mut stored = vec![1, 1];
		stored.extend_from_slice(&[7; VALUE_POINTER_SIZE]);
		let pointer = decode_value(Kind::Set, &stored).unwrap().unwrap();
		assert!(pointer.pointer);
		assert_eq!(pointer.payload, [7; VALUE_POINTER_SIZE]);
		assert!(decode_value(Kind::Set, &stored[..stored.len() - 1]).is_err());
	}

	#[test]
	fn a_hard_delete_has_no_value_and_a_range_delete_is_refused() {
		assert_eq!(decode_value(Kind::Delete, &[]).unwrap(), None);
		assert_eq!(decode_value(Kind::SoftDelete, &[]).unwrap(), None);
		assert!(matches!(decode_value(Kind::RangeDelete, b"end"), Err(Error::Unsupported(_))));
		assert!(matches!(decode_value(Kind::Set, &[]), Err(Error::Corrupted(_))));
		assert!(matches!(decode_value(Kind::Set, &[0]), Err(Error::Corrupted(_))));
		// A soft delete that does carry a value location, which no release wrote, is one.
		assert!(decode_value(Kind::SoftDelete, &[0, 1]).unwrap().is_some());
	}
}
