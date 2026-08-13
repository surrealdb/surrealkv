//! Durable branch-native codecs. All integer fields are big-endian and every
//! decoder rejects truncation, trailing bytes, invalid tags, and configured
//! allocation bounds.

use std::cmp::Ordering;

use bytes::{BufMut, Bytes, BytesMut};

use super::api::{CommitTimestamp, CommitVersion, ErrorCode, KernelError, KernelResult};

pub(crate) const FORMAT_MAGIC: [u8; 8] = *b"SKVBRNCH";
pub(crate) const FORMAT_VERSION: u16 = 1;
pub(crate) const MAX_USER_KEY_LEN: usize = 16 * 1024 * 1024;
pub(crate) const MAX_VALUE_LEN: usize = 64 * 1024 * 1024;
const INTERNAL_SUFFIX_LEN: usize = 9;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum RowKind {
	Value = 0,
	Tombstone = 1,
}

impl RowKind {
	fn decode(value: u8) -> KernelResult<Self> {
		match value {
			0 => Ok(Self::Value),
			1 => Ok(Self::Tombstone),
			_ => Err(corruption("invalid row kind")),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InternalKey {
	pub(crate) user_key: Bytes,
	pub(crate) version: CommitVersion,
	pub(crate) kind: RowKind,
}

impl InternalKey {
	pub(crate) fn new(
		user_key: Bytes,
		version: CommitVersion,
		kind: RowKind,
	) -> KernelResult<Self> {
		if user_key.len() > MAX_USER_KEY_LEN {
			return Err(KernelError::new(ErrorCode::ResourceExhausted, "user key exceeds limit"));
		}
		Ok(Self {
			user_key,
			version,
			kind,
		})
	}

	pub(crate) fn encoded_len(&self) -> usize {
		self.user_key.len() + INTERNAL_SUFFIX_LEN
	}

	pub(crate) fn encode(&self) -> Bytes {
		let mut output = BytesMut::with_capacity(self.encoded_len());
		output.extend_from_slice(&self.user_key);
		output.put_u64(!self.version.0);
		output.put_u8(self.kind as u8);
		output.freeze()
	}

	pub(crate) fn decode(encoded: Bytes) -> KernelResult<Self> {
		if encoded.len() < INTERNAL_SUFFIX_LEN {
			return Err(corruption("internal key is truncated"));
		}
		let user_len = encoded.len() - INTERNAL_SUFFIX_LEN;
		if user_len > MAX_USER_KEY_LEN {
			return Err(corruption("user key exceeds limit"));
		}
		let version_offset = user_len;
		let descending = u64::from_be_bytes(
			encoded[version_offset..version_offset + 8]
				.try_into()
				.map_err(|_| corruption("internal version is truncated"))?,
		);
		let kind = RowKind::decode(encoded[encoded.len() - 1])?;
		Ok(Self {
			user_key: encoded.slice(..user_len),
			version: CommitVersion(!descending),
			kind,
		})
	}
}

/// Semantic ordering used by builders and readers. Raw encoded bytes are not
/// compared because a user key may be a prefix of another user key.
pub(crate) fn compare_internal(left: &InternalKey, right: &InternalKey) -> Ordering {
	left.user_key
		.cmp(&right.user_key)
		.then_with(|| right.version.cmp(&left.version))
		.then_with(|| (left.kind as u8).cmp(&(right.kind as u8)))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StorageRow {
	pub(crate) key: InternalKey,
	pub(crate) commit_timestamp: CommitTimestamp,
	pub(crate) expires_at: Option<CommitTimestamp>,
	pub(crate) value: Bytes,
}

impl StorageRow {
	pub(crate) fn new(
		key: InternalKey,
		commit_timestamp: CommitTimestamp,
		expires_at: Option<CommitTimestamp>,
		value: Bytes,
	) -> KernelResult<Self> {
		if value.len() > MAX_VALUE_LEN {
			return Err(KernelError::new(ErrorCode::ResourceExhausted, "value exceeds limit"));
		}
		if key.kind == RowKind::Tombstone && (!value.is_empty() || expires_at.is_some()) {
			return Err(KernelError::new(
				ErrorCode::InvalidArgument,
				"tombstones cannot contain values or expiry",
			));
		}
		Ok(Self {
			key,
			commit_timestamp,
			expires_at,
			value,
		})
	}

	pub(crate) fn encode(&self) -> KernelResult<Bytes> {
		let internal = self.key.encode();
		let internal_len = u32::try_from(internal.len()).map_err(|_| {
			KernelError::new(ErrorCode::ResourceExhausted, "internal key is too large")
		})?;
		let value_len = u32::try_from(self.value.len())
			.map_err(|_| KernelError::new(ErrorCode::ResourceExhausted, "value is too large"))?;
		let capacity = 4usize
			.checked_add(internal.len())
			.and_then(|len| len.checked_add(8 + 1 + 8 + 4))
			.and_then(|len| len.checked_add(self.value.len()))
			.ok_or_else(|| KernelError::new(ErrorCode::ResourceExhausted, "row is too large"))?;
		let mut output = BytesMut::with_capacity(capacity);
		output.put_u32(internal_len);
		output.extend_from_slice(&internal);
		output.put_u64(self.commit_timestamp.0);
		match self.expires_at {
			Some(timestamp) => {
				output.put_u8(1);
				output.put_u64(timestamp.0);
			}
			None => {
				output.put_u8(0);
				output.put_u64(0);
			}
		}
		output.put_u32(value_len);
		output.extend_from_slice(&self.value);
		Ok(output.freeze())
	}

	pub(crate) fn decode(encoded: Bytes) -> KernelResult<Self> {
		let mut cursor = Cursor::new(encoded);
		let internal_len = cursor.u32()? as usize;
		if internal_len > MAX_USER_KEY_LEN + INTERNAL_SUFFIX_LEN {
			return Err(corruption("internal key exceeds limit"));
		}
		let key = InternalKey::decode(cursor.bytes(internal_len)?)?;
		let commit_timestamp = CommitTimestamp(cursor.u64()?);
		let expiry_tag = cursor.u8()?;
		let expiry_value = CommitTimestamp(cursor.u64()?);
		let expires_at = match expiry_tag {
			0 if expiry_value.0 == 0 => None,
			0 => return Err(corruption("absent expiry has non-zero payload")),
			1 => Some(expiry_value),
			_ => return Err(corruption("invalid expiry tag")),
		};
		let value_len = cursor.u32()? as usize;
		if value_len > MAX_VALUE_LEN {
			return Err(corruption("value exceeds limit"));
		}
		let value = cursor.bytes(value_len)?;
		if !cursor.is_empty() {
			return Err(corruption("row contains trailing bytes"));
		}
		Self::new(key, commit_timestamp, expires_at, value)
			.map_err(|_| corruption("invalid durable row combination"))
	}
}

struct Cursor {
	bytes: Bytes,
	offset: usize,
}

impl Cursor {
	fn new(bytes: Bytes) -> Self {
		Self {
			bytes,
			offset: 0,
		}
	}

	fn bytes(&mut self, len: usize) -> KernelResult<Bytes> {
		let end =
			self.offset.checked_add(len).ok_or_else(|| corruption("field length overflow"))?;
		if end > self.bytes.len() {
			return Err(corruption("durable field is truncated"));
		}
		let output = self.bytes.slice(self.offset..end);
		self.offset = end;
		Ok(output)
	}

	fn u8(&mut self) -> KernelResult<u8> {
		Ok(self.bytes(1)?[0])
	}

	fn u32(&mut self) -> KernelResult<u32> {
		Ok(u32::from_be_bytes(
			self.bytes(4)?[..].try_into().map_err(|_| corruption("u32 is truncated"))?,
		))
	}

	fn u64(&mut self) -> KernelResult<u64> {
		Ok(u64::from_be_bytes(
			self.bytes(8)?[..].try_into().map_err(|_| corruption("u64 is truncated"))?,
		))
	}

	fn is_empty(&self) -> bool {
		self.offset == self.bytes.len()
	}
}

fn corruption(message: &'static str) -> KernelError {
	KernelError::new(ErrorCode::Corruption, message)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn row(kind: RowKind, value: &'static [u8]) -> StorageRow {
		StorageRow::new(
			InternalKey::new(Bytes::from_static(b"k"), CommitVersion(5), kind).unwrap(),
			CommitTimestamp(9),
			None,
			Bytes::from_static(value),
		)
		.unwrap()
	}

	#[test]
	fn internal_key_has_byte_exact_branch_free_golden() {
		let key = row(RowKind::Value, b"v").key.encode();
		assert_eq!(key.as_ref(), &[b'k', 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfa, 0x00]);
		assert_eq!(InternalKey::decode(key).unwrap(), row(RowKind::Value, b"v").key);
	}

	#[test]
	fn semantic_comparator_handles_prefix_keys_and_descending_versions() {
		let a_old =
			InternalKey::new(Bytes::from_static(b"a"), CommitVersion(1), RowKind::Value).unwrap();
		let a_new =
			InternalKey::new(Bytes::from_static(b"a"), CommitVersion(2), RowKind::Value).unwrap();
		let aa =
			InternalKey::new(Bytes::from_static(b"aa"), CommitVersion(3), RowKind::Value).unwrap();
		assert_eq!(compare_internal(&a_new, &a_old), Ordering::Less);
		assert_eq!(compare_internal(&a_old, &aa), Ordering::Less);
	}

	#[test]
	fn row_round_trip_and_malformed_inputs_fail_closed() {
		let expected = row(RowKind::Value, b"value");
		let encoded = expected.encode().unwrap();
		assert_eq!(StorageRow::decode(encoded.clone()).unwrap(), expected);

		for cut in 0..encoded.len() {
			assert_eq!(
				StorageRow::decode(encoded.slice(..cut)).unwrap_err().code,
				ErrorCode::Corruption,
				"truncation at byte {cut} must fail"
			);
		}
		let mut trailing = BytesMut::from(encoded.as_ref());
		trailing.put_u8(0);
		assert_eq!(StorageRow::decode(trailing.freeze()).unwrap_err().code, ErrorCode::Corruption);
	}

	#[test]
	fn tombstone_payload_is_rejected_on_write_and_read() {
		let key = InternalKey::new(Bytes::from_static(b"k"), CommitVersion(1), RowKind::Tombstone)
			.unwrap();
		assert_eq!(
			StorageRow::new(key, CommitTimestamp(1), None, Bytes::from_static(b"illegal"),)
				.unwrap_err()
				.code,
			ErrorCode::InvalidArgument
		);

		let mut encoded = row(RowKind::Value, b"x").encode().unwrap().to_vec();
		let kind_offset = 4 + b"k".len() + 8;
		encoded[kind_offset] = RowKind::Tombstone as u8;
		assert_eq!(
			StorageRow::decode(Bytes::from(encoded)).unwrap_err().code,
			ErrorCode::Corruption
		);
	}
}
