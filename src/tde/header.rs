//! The cleartext header of an encrypted file.
//!
//! Every encrypted file starts its encrypted part with this header (in an SSTable, a value-log
//! file and the manifest it is the first thing in the file; the WAL carries it in its segment
//! header record). It names the suite, the key id, the file and the database, and carries the
//! salt the file key was derived with. Nothing in it is secret.
//!
//! # Layout
//!
//! 92 bytes, all integers big-endian, identical on every platform:
//!
//! ```text
//!  0  4  magic "SKTD"
//!  4  1  format version, 1
//!  5  1  suite id
//!  6  1  file kind id
//!  7  1  reserved, zero
//!  8  4  key id
//! 12  2  wrapped key length, zero in version 1
//! 14  2  reserved, zero
//! 16 16  database id
//! 32  8  file id
//! 40 32  salt
//! 72 16  tag
//! 88  4  CRC-32 (IEEE) of bytes 0..88
//! ```
//!
//! The wrapped key length is for a later mode in which a key service wraps the file key and the
//! wrapped key follows the header. Version 1 has none, so a header with a non-zero length is
//! refused instead of being read as if the key were derived.
//!
//! # Authentication
//!
//! The tag is an AEAD seal of no plaintext under a key made only for the header, with bytes 0..72
//! and any `covered` bytes the file adds as the associated data. `covered` is how a file's own
//! header fields, which are not part of this one, are authenticated too. The tag therefore never
//! uses one of the data key's nonces.
//!
//! The CRC tells media corruption from a wrong key. A header whose CRC does not match is
//! `MalformedHeader`; one whose CRC matches but whose tag does not is `WrongKey`, since whoever
//! rewrote a field and fixed the CRC did not have the key, and a reader holding the wrong key
//! sees the same thing.

use super::cipher::{open_header_tag, seal_header_tag, TAG_LEN};
use super::identity::{DbId, FileIdentity, FileKind, DB_ID_LEN};
use super::kdf::FileKey;
use super::random::SALT_LEN;
use super::suite::CipherSuite;
use crate::error::EncryptionError;

/// The first four bytes of an encrypted file's header.
pub const HEADER_MAGIC: [u8; 4] = *b"SKTD";

/// The only format version this build reads and writes.
pub const HEADER_VERSION: u8 = 1;

/// Length in bytes of the header.
pub const HEADER_LEN: usize = 92;

/// Length of the fields before the tag, which the tag authenticates.
const FIELDS_LEN: usize = 72;

/// Offset of the CRC, which covers everything before it.
const CRC_OFFSET: usize = FIELDS_LEN + TAG_LEN;

const _: () = assert!(CRC_OFFSET + 4 == HEADER_LEN);
const _: () = assert!(FIELDS_LEN == 4 + 1 + 1 + 1 + 1 + 4 + 2 + 2 + DB_ID_LEN + 8 + SALT_LEN);

/// The decoded header of an encrypted file.
///
/// What `decode` returns is only well formed, not authentic: nothing in it can be trusted until
/// `verify` has checked the tag, which `EncryptionContext::open_file` does.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CryptoHeader {
	/// The suite, kind, key id, file id and database the file belongs to.
	pub identity: FileIdentity,
	/// The salt the file key was derived with.
	pub salt: [u8; SALT_LEN],
	/// The tag that authenticates the other fields.
	pub tag: [u8; TAG_LEN],
}

impl CryptoHeader {
	/// Whether `bytes` start the way an encrypted file's header does, which is how a reader tells
	/// an encrypted file from a plaintext one. It says nothing about whether the header is valid.
	pub fn has_magic(bytes: &[u8]) -> bool {
		bytes.starts_with(&HEADER_MAGIC)
	}

	/// Makes the header of a file whose key was just generated, spending the key's header key.
	///
	/// `covered` is whatever else the file wants authenticated along with the header, such as the
	/// fields of its own header; it is not stored here, and `verify` needs the same bytes.
	///
	/// Fails with `InvalidInput` for a key made for an existing file, and with `PositionReuse`
	/// if the key's header key was already used.
	pub fn seal(key: &mut FileKey, covered: &[u8]) -> Result<Self, EncryptionError> {
		if !key.can_seal() {
			return Err(EncryptionError::InvalidInput(
				"a key made for an existing file cannot make a header",
			));
		}
		let identity = key.identity();
		let salt = *key.salt();
		let fields = encode_fields(&identity, &salt);
		let header_key = key.take_header_key()?;
		let tag = seal_header_tag(identity.suite, &header_key, &authenticated(&fields, covered))?;
		Ok(Self {
			identity,
			salt,
			tag,
		})
	}

	/// The bytes of the header, with the CRC.
	pub fn encode(&self) -> [u8; HEADER_LEN] {
		let mut out = [0u8; HEADER_LEN];
		out[..FIELDS_LEN].copy_from_slice(&encode_fields(&self.identity, &self.salt));
		out[FIELDS_LEN..CRC_OFFSET].copy_from_slice(&self.tag);
		let crc = crc32fast::hash(&out[..CRC_OFFSET]);
		out[CRC_OFFSET..].copy_from_slice(&crc.to_be_bytes());
		out
	}

	/// Reads a header from the start of `bytes`, ignoring whatever follows it.
	///
	/// Checks, in this order: the magic and a supported version (`UnsupportedVersion`, decided
	/// before anything else so that a later version may lay out the rest differently), the length
	/// and the CRC (`MalformedHeader`, media corruption), then a supported suite
	/// (`UnsupportedSuite`), a known file kind, zero reserved bytes, a zero wrapped key length and
	/// a non-zero salt (`MalformedHeader`). It never indexes past the input.
	pub fn decode(bytes: &[u8]) -> Result<Self, EncryptionError> {
		let mut start = Cursor(bytes);
		if start.take::<4>()? != HEADER_MAGIC {
			return Err(EncryptionError::MalformedHeader);
		}
		if start.take::<1>()? != [HEADER_VERSION] {
			return Err(EncryptionError::UnsupportedVersion);
		}

		let header = bytes.first_chunk::<HEADER_LEN>().ok_or(EncryptionError::MalformedHeader)?;
		let (covered, crc) =
			header.split_last_chunk::<4>().ok_or(EncryptionError::MalformedHeader)?;
		if crc32fast::hash(covered).to_be_bytes() != *crc {
			return Err(EncryptionError::MalformedHeader);
		}

		let mut fields = Cursor(covered);
		fields.take::<4>()?; // magic, checked
		fields.take::<1>()?; // version, checked
		let suite = CipherSuite::from_id(fields.take::<1>()?[0])?;
		let file_kind = FileKind::from_id(fields.take::<1>()?[0])?;
		if fields.take::<1>()? != [0] {
			return Err(EncryptionError::MalformedHeader);
		}
		let key_id = u32::from_be_bytes(fields.take::<4>()?);
		if fields.take::<2>()? != [0, 0] {
			return Err(EncryptionError::MalformedHeader);
		}
		if fields.take::<2>()? != [0, 0] {
			return Err(EncryptionError::MalformedHeader);
		}
		let db_id = DbId::from_bytes(fields.take::<DB_ID_LEN>()?);
		let file_id = u64::from_be_bytes(fields.take::<8>()?);
		let salt = fields.take::<SALT_LEN>()?;
		let tag = fields.take::<TAG_LEN>()?;
		if salt.iter().all(|&byte| byte == 0) {
			return Err(EncryptionError::MalformedHeader);
		}
		Ok(Self {
			identity: FileIdentity::new(suite, file_kind, key_id, file_id, db_id),
			salt,
			tag,
		})
	}

	/// Checks the tag with the header key of `key`, which must have been derived from exactly
	/// this header's identity and salt, and spends the header key.
	///
	/// `covered` must be the bytes the header was made with. A tag that does not match is
	/// `WrongKey`: the header is well formed, so the key is not the one the file was written with
	/// or a field was rewritten by someone without it.
	pub fn verify(&self, key: &mut FileKey, covered: &[u8]) -> Result<(), EncryptionError> {
		if key.identity() != self.identity || key.salt() != &self.salt {
			return Err(EncryptionError::InvalidInput("the key was not derived from this header"));
		}
		let fields = encode_fields(&self.identity, &self.salt);
		let header_key = key.take_header_key()?;
		open_header_tag(
			self.identity.suite,
			&header_key,
			&authenticated(&fields, covered),
			&self.tag,
		)
		.map_err(|err| match err {
			EncryptionError::AuthenticationFailed => EncryptionError::WrongKey,
			other => other,
		})
	}
}

/// The bytes before the tag, as they are laid out in the header and authenticated.
fn encode_fields(identity: &FileIdentity, salt: &[u8; SALT_LEN]) -> [u8; FIELDS_LEN] {
	let mut out = [0u8; FIELDS_LEN];
	out[0..4].copy_from_slice(&HEADER_MAGIC);
	out[4] = HEADER_VERSION;
	out[5] = identity.suite.id();
	out[6] = identity.file_kind.id();
	// 7 is reserved.
	out[8..12].copy_from_slice(&identity.key_id.to_be_bytes());
	// 12..14 is the wrapped key length, zero, and 14..16 is reserved.
	out[16..32].copy_from_slice(identity.db_id.as_bytes());
	out[32..40].copy_from_slice(&identity.file_id.to_be_bytes());
	out[40..72].copy_from_slice(salt);
	out
}

/// What the tag authenticates: the header's fields, then whatever the file adds.
fn authenticated(fields: &[u8; FIELDS_LEN], covered: &[u8]) -> Vec<u8> {
	let mut out = Vec::with_capacity(FIELDS_LEN + covered.len());
	out.extend_from_slice(fields);
	out.extend_from_slice(covered);
	out
}

/// Reads fixed-size pieces off the front of a slice without ever indexing.
struct Cursor<'a>(&'a [u8]);

impl Cursor<'_> {
	fn take<const N: usize>(&mut self) -> Result<[u8; N], EncryptionError> {
		let (chunk, rest) =
			self.0.split_first_chunk::<N>().ok_or(EncryptionError::MalformedHeader)?;
		self.0 = rest;
		Ok(*chunk)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::tde::keys::ROOT_KEY_LEN;

	const ROOT: [u8; ROOT_KEY_LEN] = {
		let mut root = [0u8; ROOT_KEY_LEN];
		let mut i = 0;
		while i < ROOT_KEY_LEN {
			root[i] = i as u8;
			i += 1;
		}
		root
	};

	const SALT: [u8; SALT_LEN] = {
		let mut salt = [0u8; SALT_LEN];
		let mut i = 0;
		while i < SALT_LEN {
			salt[i] = 0x20 + i as u8;
			i += 1;
		}
		salt
	};

	fn db_id() -> DbId {
		DbId::from_bytes(std::array::from_fn(|i| 0xA0 + i as u8))
	}

	fn identity_a() -> FileIdentity {
		FileIdentity::new(CipherSuite::Aes256Gcm, FileKind::Sst, 7, 0x0102_0304_0506_0708, db_id())
	}

	fn identity_b() -> FileIdentity {
		FileIdentity::new(
			CipherSuite::XChaCha20Poly1305,
			FileKind::Wal,
			0x0102_0304,
			0x1112_1314_1516_1718,
			db_id(),
		)
	}

	fn key(identity: FileIdentity, can_seal: bool) -> FileKey {
		FileKey::derive_for_test(&ROOT, SALT, identity, can_seal)
	}

	fn hex(bytes: &[u8]) -> String {
		bytes.iter().map(|b| format!("{b:02x}")).collect()
	}

	/// The header bytes with the CRC recomputed, so a change to a field reaches the checks after
	/// the CRC instead of stopping at it.
	fn with_fixed_crc(mut bytes: [u8; HEADER_LEN]) -> [u8; HEADER_LEN] {
		let crc = crc32fast::hash(&bytes[..CRC_OFFSET]);
		bytes[CRC_OFFSET..].copy_from_slice(&crc.to_be_bytes());
		bytes
	}

	fn header_a() -> [u8; HEADER_LEN] {
		CryptoHeader::seal(&mut key(identity_a(), true), b"").unwrap().encode()
	}

	const COVERED_B: &[u8] = b"covered by the tag";

	// Expected values from an independent computation in Python (`hmac`/`hashlib` for HKDF-SHA256,
	// `zlib.crc32` for the CRC, and pure-Python AES-256-GCM and XChaCha20-Poly1305 that were first
	// checked against the NIST and IETF vectors): the header key for label "hdr", then the tag as
	// the AEAD seal of no plaintext with nonce zero and bytes 0..72 followed by the covered bytes
	// as associated data, then the CRC over bytes 0..88. Inputs: root bytes 00..1f, salt bytes
	// 20..3f, database id bytes a0..af.
	const HEADER_A: &str = "534b5444010101000000000700000000a0a1a2a3a4a5a6a7a8a9aaabacadaeaf\
		0102030405060708202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\
		910485e98d44ad6a6260d599705e908e9a970b74";
	const HEADER_B: &str = "534b5444010203000102030400000000a0a1a2a3a4a5a6a7a8a9aaabacadaeaf\
		1112131415161718202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\
		cad606c4363a6b0fce40de2f68ee49a11dc4b4a5";

	#[test]
	fn the_header_matches_independently_computed_vectors() {
		let a = CryptoHeader::seal(&mut key(identity_a(), true), b"").unwrap();
		assert_eq!(hex(&a.encode()), HEADER_A);
		assert_eq!(hex(&a.tag), "910485e98d44ad6a6260d599705e908e");

		let b = CryptoHeader::seal(&mut key(identity_b(), true), COVERED_B).unwrap();
		assert_eq!(hex(&b.encode()), HEADER_B);
		assert_eq!(hex(&b.tag), "cad606c4363a6b0fce40de2f68ee49a1");
	}

	#[test]
	fn the_layout_is_documented_field_by_field() {
		let header = header_a();
		assert_eq!(header.len(), 92);
		assert_eq!(header[0..4], *b"SKTD");
		assert_eq!(header[4], 1);
		assert_eq!(header[5], 1);
		assert_eq!(header[6], 1);
		assert_eq!(header[7], 0);
		assert_eq!(header[8..12], [0, 0, 0, 7]);
		assert_eq!(header[12..14], [0, 0]);
		assert_eq!(header[14..16], [0, 0]);
		assert_eq!(header[16..32], (0xA0u8..0xB0).collect::<Vec<_>>()[..]);
		assert_eq!(header[32..40], [1, 2, 3, 4, 5, 6, 7, 8]);
		assert_eq!(header[40..72], SALT);
		assert_eq!(header[88..92], crc32fast::hash(&header[..88]).to_be_bytes());
	}

	#[test]
	fn a_header_round_trips_and_verifies_under_the_right_key() {
		for (identity, covered) in [(identity_a(), &b""[..]), (identity_b(), COVERED_B)] {
			let header = CryptoHeader::seal(&mut key(identity, true), covered).unwrap();
			let decoded = CryptoHeader::decode(&header.encode()).unwrap();
			assert_eq!(decoded, header);
			assert_eq!(decoded.identity, identity);
			assert_eq!(decoded.salt, SALT);

			// A reader derives its key from what the header says.
			let mut reader = key(decoded.identity, false);
			decoded.verify(&mut reader, covered).unwrap();
		}
	}

	#[test]
	fn decoding_ignores_what_follows_the_header() {
		let mut bytes = header_a().to_vec();
		bytes.extend_from_slice(&[0xEE; 40]);
		assert_eq!(CryptoHeader::decode(&bytes).unwrap().identity, identity_a());
	}

	#[test]
	fn a_truncated_header_is_malformed_at_every_length() {
		let header = header_a();
		for len in 0..HEADER_LEN {
			assert_eq!(
				CryptoHeader::decode(&header[..len]).unwrap_err(),
				EncryptionError::MalformedHeader,
				"length {len}"
			);
		}
	}

	#[test]
	fn a_header_starts_with_the_magic_and_is_refused_without_it() {
		let header = header_a();
		assert!(CryptoHeader::has_magic(&header));
		assert!(!CryptoHeader::has_magic(&header[..3]));
		assert!(!CryptoHeader::has_magic(b"VLOG plaintext vlog file"));
		for byte in 0..4 {
			let mut bad = header;
			bad[byte] ^= 0x01;
			assert!(!CryptoHeader::has_magic(&bad));
			assert_eq!(CryptoHeader::decode(&bad).unwrap_err(), EncryptionError::MalformedHeader);
			assert_eq!(
				CryptoHeader::decode(&with_fixed_crc(bad)).unwrap_err(),
				EncryptionError::MalformedHeader
			);
		}
	}

	#[test]
	fn another_version_is_unsupported_whatever_the_crc_says() {
		let header = header_a();
		for version in [0u8, 2, 3, 255] {
			let mut other = header;
			other[4] = version;
			let expected = EncryptionError::UnsupportedVersion;
			assert_eq!(CryptoHeader::decode(&other).unwrap_err(), expected, "version {version}");
			assert_eq!(CryptoHeader::decode(&with_fixed_crc(other)).unwrap_err(), expected);
			// Even a header of another version that is shorter than this one.
			assert_eq!(CryptoHeader::decode(&other[..6]).unwrap_err(), expected);
		}
	}

	#[test]
	fn a_changed_byte_without_a_fixed_crc_is_media_corruption() {
		let header = header_a();
		// Every byte after the version, flipped one bit at a time, including the CRC itself.
		for byte in 5..HEADER_LEN {
			for bit in 0..8 {
				let mut bad = header;
				bad[byte] ^= 1 << bit;
				assert_eq!(
					CryptoHeader::decode(&bad).unwrap_err(),
					EncryptionError::MalformedHeader,
					"byte {byte} bit {bit}"
				);
			}
		}
	}

	#[test]
	fn a_well_formed_but_invalid_field_is_refused_for_what_it_is() {
		let header = header_a();
		let decode = |change: &dyn Fn(&mut [u8; HEADER_LEN])| {
			let mut bad = header;
			change(&mut bad);
			CryptoHeader::decode(&with_fixed_crc(bad))
		};

		// Suites: 3 is reserved forever, 0 and everything above 2 were never assigned.
		for suite in [0u8, 3, 4, 255] {
			assert_eq!(
				decode(&|h| h[5] = suite).unwrap_err(),
				EncryptionError::UnsupportedSuite,
				"suite {suite}"
			);
		}
		// Every valid suite and kind still decodes, so the refusals above are about the value.
		assert_eq!(decode(&|h| h[5] = 2).unwrap().identity.suite, CipherSuite::XChaCha20Poly1305);
		for (kind, expected) in [(1u8, FileKind::Sst), (2, FileKind::Vlog), (3, FileKind::Wal)] {
			assert_eq!(decode(&|h| h[6] = kind).unwrap().identity.file_kind, expected);
		}
		assert_eq!(decode(&|h| h[6] = 4).unwrap().identity.file_kind, FileKind::Manifest);
		for kind in [0u8, 5, 255] {
			assert_eq!(
				decode(&|h| h[6] = kind).unwrap_err(),
				EncryptionError::MalformedHeader,
				"kind {kind}"
			);
		}
		// Reserved bytes must be zero.
		for at in [7usize, 14, 15] {
			assert_eq!(
				decode(&|h| h[at] = 1).unwrap_err(),
				EncryptionError::MalformedHeader,
				"reserved byte {at}"
			);
		}
		// A wrapped key length is not something version 1 can read.
		for at in [12usize, 13] {
			assert_eq!(
				decode(&|h| h[at] = 1).unwrap_err(),
				EncryptionError::MalformedHeader,
				"wrapped length byte {at}"
			);
		}
		// An all-zero salt would be a broken random number generator.
		assert_eq!(decode(&|h| h[40..72].fill(0)).unwrap_err(), EncryptionError::MalformedHeader);
		// One non-zero salt byte is enough.
		decode(&|h| {
			h[40..72].fill(0);
			h[71] = 1;
		})
		.unwrap();
	}

	/// What a reader that holds the right root derives from a header and checks it against.
	fn verify_as_reader(header: &CryptoHeader, covered: &[u8]) -> Result<(), EncryptionError> {
		let mut reader = FileKey::derive_for_test(&ROOT, header.salt, header.identity, false);
		header.verify(&mut reader, covered)
	}

	#[test]
	fn a_rewritten_field_with_a_fixed_crc_fails_the_tag_and_is_a_wrong_key() {
		for (identity, covered) in [(identity_a(), &b""[..]), (identity_b(), COVERED_B)] {
			let header = CryptoHeader::seal(&mut key(identity, true), covered).unwrap().encode();
			// Control: untouched, it verifies.
			verify_as_reader(&CryptoHeader::decode(&header).unwrap(), covered).unwrap();

			// Every byte of the suite, kind, key id, database id, file id, salt and tag that the
			// tag covers or is: flipped, the CRC fixed, so decoding succeeds and only the tag can
			// object. The suite and kind are changed to the other valid value.
			let offsets = [5usize, 6, 8, 11, 16, 31, 32, 39, 40, 71, 72, 87];
			for at in offsets {
				let mut bad = header;
				bad[at] = match at {
					// The other suite: 1 and 2 swap.
					5 => 3 - header[5],
					// Another kind that exists: a vlog file, or an SSTable if it already is one.
					6 if header[6] == 2 => 1,
					6 => 2,
					_ => header[at] ^ 1,
				};
				let tampered = CryptoHeader::decode(&with_fixed_crc(bad))
					.unwrap_or_else(|err| panic!("byte {at}: the CRC was fixed, got {err:?}"));
				assert_eq!(
					verify_as_reader(&tampered, covered).unwrap_err(),
					EncryptionError::WrongKey,
					"byte {at}"
				);
				// Without the fixed CRC the same change stops at the CRC, which is a different
				// error: the two tests cannot be mistaken for each other.
				assert_eq!(
					CryptoHeader::decode(&bad).unwrap_err(),
					EncryptionError::MalformedHeader,
					"byte {at}"
				);
			}
		}
	}

	#[test]
	fn the_header_is_only_valid_with_the_bytes_it_was_made_covering() {
		let sealed = |covered: &[u8]| CryptoHeader::seal(&mut key(identity_b(), true), covered);
		let header = sealed(COVERED_B).unwrap();
		verify_as_reader(&header, COVERED_B).unwrap();
		let others: [&[u8]; 4] =
			[b"", b"covered by the tag.", b"Covered by the tag", b"covered by the ta"];
		for other in others {
			assert_eq!(verify_as_reader(&header, other).unwrap_err(), EncryptionError::WrongKey);
		}
		// And the other way round: made covering nothing, it does not verify covering something.
		let bare = sealed(b"").unwrap();
		verify_as_reader(&bare, b"").unwrap();
		assert_eq!(verify_as_reader(&bare, COVERED_B).unwrap_err(), EncryptionError::WrongKey);
	}

	#[test]
	fn a_header_does_not_verify_under_another_root() {
		let header = CryptoHeader::seal(&mut key(identity_a(), true), b"").unwrap();
		let mut other_root = ROOT;
		other_root[0] ^= 1;
		let mut reader = FileKey::derive_for_test(&other_root, header.salt, header.identity, false);
		assert_eq!(header.verify(&mut reader, b"").unwrap_err(), EncryptionError::WrongKey);
	}

	#[test]
	fn a_header_is_not_checked_with_a_key_made_for_another() {
		let header = CryptoHeader::seal(&mut key(identity_a(), true), b"").unwrap();
		// Another identity, or another salt: a caller error, caught before any tag is computed and
		// without spending the key.
		let mut other_salt = SALT;
		other_salt[0] ^= 1;
		for mut wrong in [
			FileKey::derive_for_test(&ROOT, SALT, identity_b(), false),
			FileKey::derive_for_test(&ROOT, other_salt, identity_a(), false),
		] {
			let err = header.verify(&mut wrong, b"").unwrap_err();
			assert!(matches!(err, EncryptionError::InvalidInput(_)), "{err:?}");
			assert!(wrong.header_key_for_test().is_some());
		}
	}

	#[test]
	fn the_header_key_makes_or_checks_one_tag() {
		let mut writer = key(identity_a(), true);
		CryptoHeader::seal(&mut writer, b"").unwrap();
		assert_eq!(
			CryptoHeader::seal(&mut writer, b"").unwrap_err(),
			EncryptionError::PositionReuse
		);

		let header = CryptoHeader::seal(&mut key(identity_a(), true), b"").unwrap();
		let mut reader = key(identity_a(), false);
		header.verify(&mut reader, b"").unwrap();
		assert_eq!(header.verify(&mut reader, b"").unwrap_err(), EncryptionError::PositionReuse);
	}

	#[test]
	fn a_key_made_for_an_existing_file_cannot_make_a_header() {
		let mut reader = key(identity_a(), false);
		let err = CryptoHeader::seal(&mut reader, b"").unwrap_err();
		assert!(matches!(err, EncryptionError::InvalidInput(_)), "{err:?}");
		// It was refused before the header key was touched.
		assert!(reader.header_key_for_test().is_some());
	}

	#[test]
	fn every_suite_and_kind_makes_a_header_that_verifies() {
		for suite in [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305] {
			for kind in [FileKind::Sst, FileKind::Vlog, FileKind::Wal, FileKind::Manifest] {
				let identity = FileIdentity::new(suite, kind, 1, 42, db_id());
				let header = CryptoHeader::seal(&mut key(identity, true), b"x").unwrap();
				let decoded = CryptoHeader::decode(&header.encode()).unwrap();
				verify_as_reader(&decoded, b"x").unwrap();
			}
		}
	}
}
