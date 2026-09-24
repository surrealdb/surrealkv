//! V1 internal key parsing and version extraction.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
	Set,
	Delete,
	SoftDelete,
	Separator,
	RangeDelete,
	Unknown(u8),
}

impl KeyKind {
	pub fn to_u8(self) -> u8 {
		match self {
			Self::Set => 1,
			Self::Delete => 2,
			Self::SoftDelete => 3,
			Self::Separator => 4,
			Self::RangeDelete => 5,
			Self::Unknown(v) => v,
		}
	}
}

impl From<u8> for KeyKind {
	fn from(val: u8) -> Self {
		match val {
			1 => Self::Set,
			2 => Self::Delete,
			3 => Self::SoftDelete,
			4 => Self::Separator,
			5 => Self::RangeDelete,
			other => Self::Unknown(other),
		}
	}
}

/// Parsed V1 internal key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1ParsedKey<'a> {
	pub user_key: &'a [u8],
	pub seq_num: u64,
	pub kind: KeyKind,
	pub timestamp: u64,
}

impl<'a> V1ParsedKey<'a> {
	/// Decodes a V1 16-byte suffixed internal key (`user_key || trailer_8 || timestamp_8`).
	/// If the key does not have a 16-byte trailer, it falls back to 8-byte trailer or raw user key.
	pub fn decode(encoded: &'a [u8]) -> Self {
		if encoded.len() >= 16 {
			let n = encoded.len();
			let user_key = &encoded[..n - 16];
			let trailer = u64::from_le_bytes(encoded[n - 16..n - 8].try_into().unwrap());
			let timestamp = u64::from_be_bytes(encoded[n - 8..n].try_into().unwrap());
			let kind = KeyKind::from((trailer & 0xff) as u8);
			let seq_num = trailer >> 8;
			Self {
				user_key,
				seq_num,
				kind,
				timestamp,
			}
		} else if encoded.len() >= 8 {
			let n = encoded.len();
			let user_key = &encoded[..n - 8];
			let trailer = u64::from_le_bytes(encoded[n - 8..n].try_into().unwrap());
			let kind = KeyKind::from((trailer & 0xff) as u8);
			let seq_num = trailer >> 8;
			Self {
				user_key,
				seq_num,
				kind,
				timestamp: 0,
			}
		} else {
			Self {
				user_key: encoded,
				seq_num: 0,
				kind: KeyKind::Set,
				timestamp: 0,
			}
		}
	}

	pub fn is_tombstone(&self) -> bool {
		matches!(self.kind, KeyKind::Delete | KeyKind::RangeDelete)
	}
}
