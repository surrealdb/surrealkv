//! The container of an encrypted manifest.
//!
//! A plaintext manifest starts with its format version, the two bytes `00 01`. An encrypted one
//! starts with a magic of its own, so a reader that does not know it fails at the first thing it
//! reads, which is the manifest, before it has looked at a log, a table or a value file:
//!
//! ```text
//!   0   7  magic "SKVMENC"
//!   7   1  container version, 1
//!   8   8  length of the sealed body, tag included (big-endian)
//!  16  92  crypto header (file kind manifest, file id 0)
//! 108      the sealed body: ciphertext, then the 16 byte tag
//! ```
//!
//! The body is the encoding of the manifest that a plaintext database writes, sealed as one AEAD
//! message at nonce slot 0 under a file key of its own. Nothing about the body is in the clear
//! and there is no checksum of it: the tag is the integrity check, and a write that a crash cuts
//! short never reaches it, since the manifest is replaced atomically.
//!
//! Every byte before the crypto header is passed to it as the bytes that its tag covers, so the
//! magic, the version and the length are authenticated along with the header. The file has no
//! other length: it is exactly the 108 bytes of the prefix and the length that the prefix says,
//! which is checked before the key is touched, so a file that was cut short or that has
//! something after its tag is reported as that and not as a wrong key.
//!
//! Everything up to the end of the crypto header is readable without a key. That is what
//! [`describe_manifest`] reads: whether the manifest is encrypted and, if it is, the database,
//! the cipher suite and the key id it was written with.
//!
//! # A key for each write
//!
//! The nonce is the same for every manifest, slot 0, so what keeps a (key, nonce) pair from
//! repeating is the key: [`seal_manifest`] draws a new salt, and with it a new file key, on
//! every call, and nothing keeps the cipher or the salt of one write for the next. A write that
//! failed and is tried again is a new call, so it is a new salt.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use super::cipher::{BlockContext, BlockKind, FileCipher, TAG_LEN};
use super::context::EncryptionContext;
use super::header::{CryptoHeader, HEADER_LEN};
use super::identity::{DbId, FileKind};
use super::suite::CipherSuite;
use crate::error::{EncryptionError, Error, Result};

/// The first bytes of an encrypted manifest.
pub const MANIFEST_MAGIC: [u8; 7] = *b"SKVMENC";

/// The only version of this container that this build reads and writes.
pub const MANIFEST_CONTAINER_VERSION: u8 = 1;

/// Length of what comes before the crypto header: the magic, the version and the length of the
/// sealed body.
const FIXED_LEN: usize = MANIFEST_MAGIC.len() + 1 + 8;

/// Length of everything that is readable without a key, where the sealed body starts.
pub const MANIFEST_PREFIX_LEN: usize = FIXED_LEN + HEADER_LEN;

/// The file id of the manifest. There is one, and a header that names another is not its own.
const MANIFEST_FILE_ID: u64 = 0;

/// What a manifest says about itself without a key.
///
/// Every field but `encrypted` is `None` for a manifest that is not encrypted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ManifestDescription {
	/// Whether the manifest is encrypted. A manifest that is not is not examined any further:
	/// whether it is a manifest at all is for its loader to say.
	pub encrypted: bool,
	/// The database that the manifest belongs to.
	pub db_id: Option<DbId>,
	/// The cipher suite of the database, which new files are written with.
	pub suite: Option<CipherSuite>,
	/// The key id the manifest was sealed under, which is the key that was active when it was
	/// last written. Files that were written earlier may name other key ids.
	pub key_id: Option<u32>,
}

impl ManifestDescription {
	const fn plain() -> Self {
		Self {
			encrypted: false,
			db_id: None,
			suite: None,
			key_id: None,
		}
	}
}

/// Whether `bytes`, the start of a manifest file, begin the way an encrypted manifest does. It
/// says nothing about whether the rest is well formed.
pub fn is_sealed_manifest(bytes: &[u8]) -> bool {
	bytes.starts_with(&MANIFEST_MAGIC)
}

/// Whether `bytes` are a part of the magic that is too short to say that the file is an
/// encrypted manifest: nothing, or the first bytes of it. The start of no plaintext manifest, so
/// a file like that was cut short.
pub fn is_cut_inside_manifest_magic(bytes: &[u8]) -> bool {
	bytes.len() < MANIFEST_MAGIC.len() && MANIFEST_MAGIC.starts_with(bytes)
}

fn cut_short() -> Error {
	Error::ManifestCorruption(
		"the encrypted manifest is cut short: it ends inside its header".to_string(),
	)
}

/// The fixed part of the prefix for a sealed body of `sealed_len` bytes.
fn fixed_prefix(sealed_len: u64) -> [u8; FIXED_LEN] {
	let mut fixed = [0u8; FIXED_LEN];
	let (magic, rest) = fixed.split_at_mut(MANIFEST_MAGIC.len());
	magic.copy_from_slice(&MANIFEST_MAGIC);
	if let Some((version, length)) = rest.split_first_mut() {
		*version = MANIFEST_CONTAINER_VERSION;
		length.copy_from_slice(&sealed_len.to_be_bytes());
	}
	fixed
}

/// What the prefix of an encrypted manifest holds, taken apart without a key.
struct Prefix<'a> {
	/// The bytes the header's tag covers.
	fixed: &'a [u8; FIXED_LEN],
	/// The length of the sealed body that the file says it has.
	sealed_len: u64,
	header: CryptoHeader,
}

/// Reads the prefix of an encrypted manifest, whose magic the caller has checked.
///
/// Fails with `UnsupportedVersion` for a container version this build does not know, decided
/// first, before the length, since a later version may lay the rest out differently; with a
/// corruption error for a file that ends inside the prefix; and with what
/// `CryptoHeader::decode` says for a header that is not well formed. A header that is not the
/// manifest's, another kind of file or another id, is `FileIdMismatch`.
fn parse_prefix(bytes: &[u8]) -> Result<Prefix<'_>> {
	let version = bytes.get(MANIFEST_MAGIC.len()).ok_or_else(cut_short)?;
	if *version != MANIFEST_CONTAINER_VERSION {
		return Err(EncryptionError::UnsupportedVersion.into());
	}
	let (fixed, rest) = bytes.split_first_chunk::<FIXED_LEN>().ok_or_else(cut_short)?;
	let header_bytes = rest.first_chunk::<HEADER_LEN>().ok_or_else(cut_short)?;
	let header = CryptoHeader::decode(header_bytes)?;
	if header.identity.file_kind != FileKind::Manifest
		|| header.identity.file_id != MANIFEST_FILE_ID
	{
		return Err(EncryptionError::FileIdMismatch.into());
	}
	let length = fixed.last_chunk::<8>().ok_or_else(cut_short)?;
	Ok(Prefix {
		fixed,
		sealed_len: u64::from_be_bytes(*length),
		header,
	})
}

/// Says what the start of a manifest file is, without a key.
///
/// `prefix` is the start of the file, at least its first [`MANIFEST_PREFIX_LEN`] bytes if it is
/// encrypted. What does not begin with the magic is described as not encrypted, with nothing
/// else said about it. An encrypted manifest is checked as far as it can be without a key: the
/// version, the header's own checksum and that the header is the manifest's. The tag is not, so
/// nothing here is authentic.
pub fn describe_manifest(prefix: &[u8]) -> Result<ManifestDescription> {
	if !is_sealed_manifest(prefix) {
		return Ok(ManifestDescription::plain());
	}
	let parsed = parse_prefix(prefix)?;
	let identity = parsed.header.identity;
	Ok(ManifestDescription {
		encrypted: true,
		db_id: Some(identity.db_id),
		suite: Some(identity.suite),
		key_id: Some(identity.key_id),
	})
}

/// Reads the start of the manifest file at `path` and describes it, see [`describe_manifest`].
/// Only the prefix is read, however long the file is.
#[allow(
	clippy::disallowed_methods,
	reason = "std-path tooling that reads the clear prefix of a manifest on a disk, for a tool that holds no file system handle"
)]
pub fn describe_manifest_file(path: impl AsRef<Path>) -> Result<ManifestDescription> {
	let mut prefix = Vec::with_capacity(MANIFEST_PREFIX_LEN);
	File::open(path.as_ref())?.take(MANIFEST_PREFIX_LEN as u64).read_to_end(&mut prefix)?;
	describe_manifest(&prefix)
}

/// The position and kind of the one block of a manifest.
const BODY_BLOCK: BlockContext = BlockContext::new(BlockKind::ManifestBody, 0, 0);

/// Seals `body`, the encoding of a manifest, and returns the bytes of the encrypted manifest.
///
/// A new salt is drawn for every call, so two calls with the same body give two different
/// files, and so does the second try of a write that failed. The key is the active key of
/// `ctx`. Fails with `Error::Encryption`, in particular `KeyProvider(Unavailable)`, which may be
/// tried again, before anything is written: this only builds bytes.
pub fn seal_manifest(ctx: &EncryptionContext, body: &[u8]) -> Result<Vec<u8>> {
	let sealed_len = sealed_length(body.len())?;
	let fixed = fixed_prefix(sealed_len);
	let (header, cipher) = ctx.create_file(FileKind::Manifest, MANIFEST_FILE_ID, &fixed)?;
	assemble(&fixed, &header, &cipher, body)
}

/// The length of a body of `body_len` bytes once it is sealed.
fn sealed_length(body_len: usize) -> Result<u64> {
	body_len
		.checked_add(TAG_LEN)
		.and_then(|len| u64::try_from(len).ok())
		.ok_or_else(|| EncryptionError::InvalidInput("the manifest is too long to seal").into())
}

/// Lays out the file for a header and a cipher that were made for it, and seals `body` in it.
fn assemble(
	fixed: &[u8; FIXED_LEN],
	header: &CryptoHeader,
	cipher: &FileCipher,
	body: &[u8],
) -> Result<Vec<u8>> {
	let total = MANIFEST_PREFIX_LEN
		.checked_add(body.len())
		.and_then(|len| len.checked_add(TAG_LEN))
		.ok_or(EncryptionError::InvalidInput("the manifest is too long to seal"))?;
	let mut out = Vec::new();
	out.try_reserve_exact(total).map_err(|e| Error::Io(Arc::new(std::io::Error::from(e))))?;
	out.extend_from_slice(fixed);
	out.extend_from_slice(&header.encode());
	out.extend_from_slice(body);
	let (_, sealed) = out.split_at_mut(MANIFEST_PREFIX_LEN);
	let tag = cipher.seal_detached(&BODY_BLOCK, sealed)?;
	out.extend_from_slice(&tag);
	Ok(out)
}

/// Opens an encrypted manifest and returns the body, the encoding of the manifest.
///
/// In order: the version and the header's checksum, then the length of the file against the
/// length it says, then the key: the database (`DbIdMismatch`), the key's state, the header's
/// tag (`WrongKey`), and last the body's (`AuthenticationFailed`). A file that is cut short or
/// that has more than it says is a corruption error, and is reported whatever the key. Nothing
/// is decoded before the tag has been checked, and the only allocation is the size of the file,
/// which is already in memory.
///
/// Asks the key manager of `ctx` to make the key of the file available first, which may wait on
/// a key provider: this is for opening a database or a checkpoint, never for a write.
pub fn open_manifest(ctx: &EncryptionContext, file: &[u8]) -> Result<Vec<u8>> {
	if !is_sealed_manifest(file) {
		return Err(EncryptionError::NotEncrypted.into());
	}
	let parsed = parse_prefix(file)?;
	let sealed = file.get(MANIFEST_PREFIX_LEN..).ok_or_else(cut_short)?;
	if u64::try_from(sealed.len()).ok() != Some(parsed.sealed_len) {
		let which = if u64::try_from(sealed.len()).is_ok_and(|have| have < parsed.sealed_len) {
			"the encrypted manifest is cut short"
		} else {
			"the encrypted manifest has bytes after the end that its header gives it"
		};
		return Err(Error::ManifestCorruption(format!(
			"{which}: its header says {} bytes of sealed body and the file holds {}",
			parsed.sealed_len,
			sealed.len()
		)));
	}

	ctx.prepare(&[parsed.header.identity.key_id])?;
	let cipher =
		ctx.open_file(&parsed.header, FileKind::Manifest, MANIFEST_FILE_ID, parsed.fixed)?;

	let mut body = Vec::new();
	body.try_reserve_exact(sealed.len())
		.map_err(|e| Error::Io(Arc::new(std::io::Error::from(e))))?;
	body.extend_from_slice(sealed);
	let opened = cipher.open_suffix(&BODY_BLOCK, &mut body)?.len();
	body.truncate(opened);
	Ok(body)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::tde::identity::FileIdentity;
	use crate::tde::kdf::FileKey;
	use crate::tde::keys::ROOT_KEY_LEN;
	use crate::tde::random::SALT_LEN;
	use crate::tde::SoftwareKeyManager;

	const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

	fn db_id() -> DbId {
		DbId::from_bytes(std::array::from_fn(|i| 0xA0 + i as u8))
	}

	fn context(suite: CipherSuite) -> EncryptionContext {
		EncryptionContext::new(
			Arc::new(SoftwareKeyManager::new([0x11; ROOT_KEY_LEN])),
			db_id(),
			suite,
		)
	}

	fn hex(bytes: &[u8]) -> String {
		bytes.iter().map(|b| format!("{b:02x}")).collect()
	}

	/// The body of the known-answer vectors: not a manifest, only bytes.
	const KAT_BODY: &[u8] = b"manifest body: a few bytes that stand for the encoding of one";

	/// The container of `KAT_BODY` for a root of bytes 00..1f, a salt of bytes 20..3f, database id
	/// bytes a0..af and key id 1, computed outside this code: OpenSSL's AES-256-GCM and
	/// ChaCha20-Poly1305 (with HChaCha20 written from the XChaCha draft) for the AEADs, `hmac` for
	/// HKDF-SHA256 and `zlib.crc32` for the header's CRC. The parts are the magic, the version, the
	/// length of the sealed body (77), the 92 byte header, whose tag covers the first 16 bytes,
	/// then the ciphertext and the tag.
	const KAT_AES: &str = "534b564d454e4301000000000000004d534b5444010104000000000100000000\
		a0a1a2a3a4a5a6a7a8a9aaabacadaeaf00000000000000002021222324252627\
		28292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f98c6ebe615cc4839\
		5fb7a8c3e1bf79f70f5d4918acd1ca2566b7f6d85b0abe895d8acaa162057ded\
		f060aff0d96c49f09976b63436a26182e2339e56d040f00ff3c7af6cccd5a7db\
		eaa5d8c7c19f8a87088f05ad8a9d6a372ec014ffaf34692d49";
	const KAT_XCHACHA: &str = "534b564d454e4301000000000000004d534b5444010204000000000100000000\
		a0a1a2a3a4a5a6a7a8a9aaabacadaeaf00000000000000002021222324252627\
		28292a2b2c2d2e2f303132333435363738393a3b3c3d3e3fdb0f2cf285e32333\
		b056c34a29a2b49f5d96198763cdaf751270928e92f44de88f00ff2f41745b00\
		6218826b5d7e5d69ba42d04194e1a9cd729a8c12c028644eb85cbcbb782be905\
		d18b3256ca4d38910c214bfdbd9593bacf811a25194e310ae2";

	fn kat_container(suite: CipherSuite) -> Vec<u8> {
		let root: [u8; ROOT_KEY_LEN] = std::array::from_fn(|i| i as u8);
		let salt: [u8; SALT_LEN] = std::array::from_fn(|i| 0x20 + i as u8);
		let identity = FileIdentity::new(suite, FileKind::Manifest, 1, MANIFEST_FILE_ID, db_id());
		let fixed = fixed_prefix(sealed_length(KAT_BODY.len()).unwrap());
		let mut key = FileKey::derive_for_test(&root, salt, identity, true);
		let header = CryptoHeader::seal(&mut key, &fixed).unwrap();
		let cipher = FileCipher::new(key).unwrap();
		assemble(&fixed, &header, &cipher, KAT_BODY).unwrap()
	}

	#[test]
	fn the_container_matches_independently_computed_vectors() {
		for (suite, expected) in
			[(CipherSuite::Aes256Gcm, KAT_AES), (CipherSuite::XChaCha20Poly1305, KAT_XCHACHA)]
		{
			let built = kat_container(suite);
			assert_eq!(hex(&built), expected.trim(), "{suite:?}");
			assert_eq!(built.len(), MANIFEST_PREFIX_LEN + KAT_BODY.len() + TAG_LEN);
		}
	}

	#[test]
	fn the_vectors_open_with_the_root_they_were_made_with() {
		let root: [u8; ROOT_KEY_LEN] = std::array::from_fn(|i| i as u8);
		for suite in SUITES {
			let ctx =
				EncryptionContext::new(Arc::new(SoftwareKeyManager::new(root)), db_id(), suite);
			assert_eq!(open_manifest(&ctx, &kat_container(suite)).unwrap(), KAT_BODY, "{suite:?}");
		}
	}

	#[test]
	fn a_manifest_does_not_start_like_a_plaintext_one_or_like_another_encrypted_file() {
		// A plaintext manifest starts with its version, 1, in two bytes; the old loader reads
		// those first and refuses any other number.
		assert_ne!(MANIFEST_MAGIC[..2], [0, 1]);
		assert_ne!(u16::from_be_bytes([MANIFEST_MAGIC[0], MANIFEST_MAGIC[1]]), 1);
		assert_ne!(MANIFEST_MAGIC[..4], *b"SKTD");
		assert_ne!(MANIFEST_MAGIC[..4], *b"VLOG");
		assert_eq!(MANIFEST_PREFIX_LEN, 108);
	}

	#[test]
	fn a_body_of_any_length_round_trips_under_both_suites() {
		for suite in SUITES {
			let ctx = context(suite);
			for len in [0usize, 1, 15, 16, 17, 300] {
				let body: Vec<u8> = (0..len).map(|i| (i * 7 + 3) as u8).collect();
				let sealed = seal_manifest(&ctx, &body).unwrap();
				assert_eq!(sealed.len(), MANIFEST_PREFIX_LEN + len + TAG_LEN);
				assert_eq!(&sealed[..7], b"SKVMENC");
				assert_eq!(open_manifest(&ctx, &sealed).unwrap(), body, "{suite:?} {len}");
			}
		}
	}

	#[test]
	fn nothing_of_the_body_is_in_the_clear() {
		let ctx = context(CipherSuite::XChaCha20Poly1305);
		let body = b"canary-body-canary-body-canary-body".repeat(4);
		let sealed = seal_manifest(&ctx, &body).unwrap();
		assert!(!sealed.windows(12).any(|window| window == b"canary-body-"));
		assert!(!sealed.windows(6).any(|window| window == &body[..6]));
	}

	#[test]
	fn every_write_is_sealed_under_a_new_salt() {
		for suite in SUITES {
			let ctx = context(suite);
			let first = seal_manifest(&ctx, KAT_BODY).unwrap();
			let second = seal_manifest(&ctx, KAT_BODY).unwrap();
			assert_ne!(first, second, "{suite:?}");
			let salt = |bytes: &[u8]| CryptoHeader::decode(&bytes[FIXED_LEN..]).unwrap().salt;
			assert_ne!(salt(&first), salt(&second), "{suite:?}: the salt was reused");
			// The same plaintext at the same position gives other ciphertext and another tag.
			assert_ne!(first[MANIFEST_PREFIX_LEN..], second[MANIFEST_PREFIX_LEN..], "{suite:?}");
			// Both are the same manifest.
			assert_eq!(open_manifest(&ctx, &first).unwrap(), open_manifest(&ctx, &second).unwrap());
		}
	}

	/// The error a failed open is, or a panic naming `what` if it succeeded.
	fn failure(result: Result<Vec<u8>>, what: &str) -> Error {
		match result {
			Ok(_) => panic!("{what}: the manifest opened"),
			Err(err) => err,
		}
	}

	fn encryption(err: Error, what: &str) -> EncryptionError {
		match err {
			Error::Encryption(inner) => inner,
			other => panic!("{what}: expected an encryption error, got {other:?}"),
		}
	}

	/// The bytes with the CRC of the crypto header made again, so that a change to a field of the
	/// header reaches the checks that come after the CRC.
	fn with_fixed_header_crc(mut bytes: Vec<u8>) -> Vec<u8> {
		let crc_at = MANIFEST_PREFIX_LEN - 4;
		let crc = crc32fast::hash(&bytes[FIXED_LEN..crc_at]);
		bytes[crc_at..MANIFEST_PREFIX_LEN].copy_from_slice(&crc.to_be_bytes());
		bytes
	}

	fn flipped(bytes: &[u8], at: usize) -> Vec<u8> {
		let mut changed = bytes.to_vec();
		changed[at] ^= 0x01;
		changed
	}

	/// A manifest sealed for a root of bytes 00..1f, and a context that holds that root.
	fn sealed_for_the_vectors(suite: CipherSuite) -> (EncryptionContext, Vec<u8>) {
		let root: [u8; ROOT_KEY_LEN] = std::array::from_fn(|i| i as u8);
		let ctx = EncryptionContext::new(Arc::new(SoftwareKeyManager::new(root)), db_id(), suite);
		let bytes = kat_container(suite);
		assert_eq!(open_manifest(&ctx, &bytes).unwrap(), KAT_BODY);
		(ctx, bytes)
	}

	#[test]
	fn a_changed_byte_of_the_prefix_or_the_header_is_the_error_of_that_field() {
		use EncryptionError as E;
		let magic = MANIFEST_MAGIC.len();
		for suite in SUITES {
			let (ctx, bytes) = sealed_for_the_vectors(suite);
			for at in 0..MANIFEST_PREFIX_LEN {
				let what = format!("{suite:?} byte {at}");
				let changed = flipped(&bytes, at);
				let error = failure(open_manifest(&ctx, &changed), &what);

				if at < magic {
					// It no longer starts like an encrypted manifest.
					assert_eq!(encryption(error, &what), E::NotEncrypted, "{what}");
					continue;
				}
				if at == magic {
					assert_eq!(encryption(error, &what), E::UnsupportedVersion, "{what}");
					continue;
				}
				if at < FIXED_LEN {
					// The length: the file is not as long as it says.
					assert!(matches!(error, Error::ManifestCorruption(_)), "{what}: {error:?}");
					continue;
				}

				// A byte of the crypto header. With its checksum as it was, the checksum is what
				// stops it, except for the version, which is read before the checksum.
				let field = at - FIXED_LEN;
				let stale = if field == 4 {
					E::UnsupportedVersion
				} else {
					E::MalformedHeader
				};
				assert_eq!(encryption(error, &what), stale, "{what}: stale checksum");

				// With the checksum made again the field is read as it is, and the checks after
				// the checksum speak for it. A changed checksum made again is the header as it was.
				if field >= HEADER_LEN - 4 {
					continue;
				}
				let expected = match field {
					// The magic of the header.
					0..=3 => E::MalformedHeader,
					// Its version, a suite that does not exist (0, or the reserved 3), a kind
					// that does not exist and bytes that are zero in this version: a later
					// format, which is no damage.
					4 | 6 | 7 | 12..=15 => E::UnsupportedVersion,
					5 => E::UnsupportedSuite,
					// A key id that the manager has never held.
					8..=11 => E::UnknownKeyId,
					// Another database.
					16..=31 => E::DbIdMismatch,
					// Not the file that the manifest is.
					32..=39 => E::FileIdMismatch,
					// The salt and the tag: the key that the header says is not the one it was
					// sealed with.
					_ => E::WrongKey,
				};
				let error = failure(open_manifest(&ctx, &with_fixed_header_crc(changed)), &what);
				assert_eq!(encryption(error, &what), expected, "{what}: checksum made again");
			}
		}
	}

	#[test]
	fn a_changed_byte_of_the_sealed_body_or_its_tag_is_an_authentication_failure() {
		for suite in SUITES {
			let (ctx, bytes) = sealed_for_the_vectors(suite);
			for at in MANIFEST_PREFIX_LEN..bytes.len() {
				let what = format!("{suite:?} byte {at}");
				let err = failure(open_manifest(&ctx, &flipped(&bytes, at)), &what);
				assert_eq!(encryption(err, &what), EncryptionError::AuthenticationFailed, "{what}");
			}
		}
	}

	#[test]
	fn the_length_is_covered_by_the_header_so_a_length_that_was_rewritten_with_the_file_is_not_accepted(
	) {
		for suite in SUITES {
			let (ctx, bytes) = sealed_for_the_vectors(suite);
			// One byte of the sealed body cut off, and the length that the file says made to
			// agree: nothing is cut short any more, and the header's tag, which covers the length,
			// is what stops it.
			let mut shorter = bytes[..bytes.len() - 1].to_vec();
			let sealed_len = u64::from_be_bytes(shorter[8..16].try_into().unwrap()) - 1;
			shorter[8..16].copy_from_slice(&sealed_len.to_be_bytes());
			let err = failure(open_manifest(&ctx, &shorter), "a length made to agree");
			assert_eq!(encryption(err, "shorter"), EncryptionError::WrongKey, "{suite:?}");

			// Likewise for a longer file.
			let mut longer = bytes.clone();
			longer.push(0);
			let sealed_len = u64::from_be_bytes(longer[8..16].try_into().unwrap()) + 1;
			longer[8..16].copy_from_slice(&sealed_len.to_be_bytes());
			let err = failure(open_manifest(&ctx, &longer), "a length made to agree");
			assert_eq!(encryption(err, "longer"), EncryptionError::WrongKey, "{suite:?}");
		}
	}

	#[test]
	fn a_file_that_ends_early_is_cut_short_at_every_length_and_whatever_the_key() {
		for suite in SUITES {
			let (ctx, bytes) = sealed_for_the_vectors(suite);
			let wrong_root = EncryptionContext::new(
				Arc::new(SoftwareKeyManager::new([0x99; ROOT_KEY_LEN])),
				db_id(),
				suite,
			);
			for len in 0..bytes.len() {
				let what = format!("{suite:?} {len} bytes");
				let cut = &bytes[..len];
				if len < MANIFEST_MAGIC.len() {
					// Not a manifest of any kind that this reads: a part of the magic.
					assert!(is_cut_inside_manifest_magic(cut), "{what}");
					assert!(!describe_manifest(cut).unwrap().encrypted, "{what}");
					let err = failure(open_manifest(&ctx, cut), &what);
					assert_eq!(encryption(err, &what), EncryptionError::NotEncrypted, "{what}");
					continue;
				}
				for opened_with in [&ctx, &wrong_root] {
					let err = failure(open_manifest(opened_with, cut), &what);
					assert!(
						matches!(&err, Error::ManifestCorruption(message) if message.contains("cut short")),
						"{what}: {err:?}"
					);
				}
				// What can be described without a key is described, and the rest is cut short.
				match describe_manifest(cut) {
					Ok(described) => {
						assert!(len >= MANIFEST_PREFIX_LEN, "{what}");
						assert!(described.encrypted, "{what}");
					}
					Err(err) => {
						assert!(len < MANIFEST_PREFIX_LEN, "{what}");
						assert!(
							matches!(&err, Error::ManifestCorruption(message) if message.contains("cut short")),
							"{what}: {err:?}"
						);
					}
				}
			}
		}
	}

	#[test]
	fn a_file_with_something_after_its_tag_is_refused_before_the_key_is_used() {
		for suite in SUITES {
			let (ctx, mut bytes) = sealed_for_the_vectors(suite);
			bytes.push(0);
			let err = failure(open_manifest(&ctx, &bytes), "a trailing byte");
			assert!(
				matches!(&err, Error::ManifestCorruption(message) if message.contains("after the end")),
				"{suite:?}: {err:?}"
			);
		}
	}

	#[test]
	fn a_key_that_does_not_fit_is_named_and_a_key_that_fits_opens_the_manifest() {
		for suite in SUITES {
			let (_, bytes) = sealed_for_the_vectors(suite);
			let root: [u8; ROOT_KEY_LEN] = std::array::from_fn(|i| i as u8);

			// Another root behind the same key id and database.
			let other_root = EncryptionContext::new(
				Arc::new(SoftwareKeyManager::new([0x99; ROOT_KEY_LEN])),
				db_id(),
				suite,
			);
			let err = failure(open_manifest(&other_root, &bytes), "another root");
			assert_eq!(encryption(err, "root"), EncryptionError::WrongKey, "{suite:?}");

			// The right root, in another database.
			let other_db = EncryptionContext::new(
				Arc::new(SoftwareKeyManager::new(root)),
				DbId::from_bytes([0xD1; 16]),
				suite,
			);
			let err = failure(open_manifest(&other_db, &bytes), "another database");
			assert_eq!(encryption(err, "database"), EncryptionError::DbIdMismatch, "{suite:?}");

			// A manager that has never held key 1, and one that has retired it.
			let km = SoftwareKeyManager::new(root);
			km.add_key(2, [0x22; ROOT_KEY_LEN]).unwrap();
			km.set_active_key_id(2).unwrap();
			km.retire_key(1).unwrap();
			let retired = EncryptionContext::new(Arc::new(km), db_id(), suite);
			let err = failure(open_manifest(&retired, &bytes), "a retired key");
			assert_eq!(encryption(err, "retired"), EncryptionError::KeyRetired, "{suite:?}");
		}
	}

	#[test]
	fn a_plaintext_manifest_is_not_opened_as_an_encrypted_one() {
		let ctx = context(CipherSuite::Aes256Gcm);
		let plain = [&[0u8, 1][..], &[0u8; 120][..]].concat();
		let err = failure(open_manifest(&ctx, &plain), "a plaintext manifest");
		assert_eq!(encryption(err, "plain"), EncryptionError::NotEncrypted);
	}

	#[test]
	fn a_header_of_another_file_is_not_the_manifests() {
		for suite in SUITES {
			let ctx = context(suite);
			let (header, _) = ctx.create_file(FileKind::Wal, 0, &fixed_prefix(32)).unwrap();
			let mut bytes = fixed_prefix(32).to_vec();
			bytes.extend_from_slice(&header.encode());
			bytes.extend_from_slice(&[0u8; 32]);
			let err = failure(open_manifest(&ctx, &bytes), "a header of a log segment");
			assert_eq!(encryption(err, "wal"), EncryptionError::FileIdMismatch, "{suite:?}");
			assert!(describe_manifest(&bytes).is_err());
		}
	}

	#[test]
	fn the_description_needs_no_key_and_says_what_the_manifest_was_written_with() {
		for suite in SUITES {
			let km = SoftwareKeyManager::new([0x11; ROOT_KEY_LEN]);
			km.add_key(7, [0x77; ROOT_KEY_LEN]).unwrap();
			km.set_active_key_id(7).unwrap();
			let ctx = EncryptionContext::new(Arc::new(km), db_id(), suite);
			let sealed = seal_manifest(&ctx, KAT_BODY).unwrap();

			let described = describe_manifest(&sealed).unwrap();
			assert!(described.encrypted);
			assert_eq!(described.db_id, Some(db_id()));
			assert_eq!(described.suite, Some(suite));
			assert_eq!(described.key_id, Some(7));
			// Only the prefix is needed.
			assert_eq!(describe_manifest(&sealed[..MANIFEST_PREFIX_LEN]).unwrap(), described);
		}
	}

	#[test]
	fn a_manifest_that_is_not_encrypted_is_described_as_that_and_nothing_else() {
		let plain = [&[0u8, 1][..], &[0xAB; 100][..]].concat();
		let described = describe_manifest(&plain).unwrap();
		assert!(!described.encrypted);
		assert_eq!((described.db_id, described.suite, described.key_id), (None, None, None));
		assert!(!describe_manifest(b"").unwrap().encrypted);
		assert!(!describe_manifest(b"SKVMEN").unwrap().encrypted);
		assert!(!describe_manifest(b"SKVMENX and more").unwrap().encrypted);
	}

	#[test]
	fn a_part_of_the_magic_is_a_file_that_was_cut_short_and_no_plaintext_manifest() {
		assert!(is_cut_inside_manifest_magic(b""));
		for len in 1..MANIFEST_MAGIC.len() {
			assert!(is_cut_inside_manifest_magic(&MANIFEST_MAGIC[..len]), "{len}");
			assert!(!is_sealed_manifest(&MANIFEST_MAGIC[..len]), "{len}");
		}
		assert!(!is_cut_inside_manifest_magic(&MANIFEST_MAGIC));
		assert!(!is_cut_inside_manifest_magic(&[0, 1, 0, 0]));
		assert!(!is_cut_inside_manifest_magic(b"SKVX"));
	}

	// Reads a file, which a target without a file system and without `tempfile` cannot.
	#[test]
	#[cfg(not(target_arch = "wasm32"))]
	fn the_description_reads_only_the_prefix_of_a_file() {
		let dir = tempfile::tempdir().unwrap();
		let ctx = context(CipherSuite::Aes256Gcm);
		let big = vec![0x5Au8; 20_000];
		let path = dir.path().join("0.manifest");
		std::fs::write(&path, seal_manifest(&ctx, &big).unwrap()).unwrap();
		let described = describe_manifest_file(&path).unwrap();
		assert!(described.encrypted);
		assert_eq!(described.db_id, Some(db_id()));

		std::fs::write(&path, [0u8, 1, 2, 3]).unwrap();
		assert!(!describe_manifest_file(&path).unwrap().encrypted);
		assert!(describe_manifest_file(dir.path().join("missing")).is_err());
	}
}
