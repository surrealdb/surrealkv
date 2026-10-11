//! Sealing and opening the blocks of one file.
//!
//! Every block is an AEAD message sealed in place with a detached 16 byte tag. What the block
//! is bound to is not stored with it: the reader rebuilds it from the file header and from the
//! block's position, so a block that is moved, replayed or reinterpreted fails to open.
//!
//! # Associated data
//!
//! 48 bytes, all integers big-endian, rebuilt by [`BlockContext::aad`]:
//!
//! ```text
//!  0  1  layout version, 1
//!  1  1  suite id
//!  2  1  file kind id
//!  3  1  block kind id
//!  4  4  key id
//!  8  8  file id
//! 16 16  database id
//! 32  8  position
//! 40  1  stored compression byte
//! 41  7  reserved, zero
//! ```
//!
//! The compression byte sits outside the sealed bytes in an SSTable block, so it is part of
//! the associated data: flipping it would otherwise make a reader decompress sealed plaintext
//! with the wrong algorithm.
//!
//! # Nonces
//!
//! The nonce is a function of the block's position alone, and the position is a *slot* that
//! is used at most once per file key:
//!
//! - AES-256-GCM, 96 bits: `00 00 00 00 || position (8 bytes, BE)`.
//! - XChaCha20-Poly1305, 192 bits: `00 .. 00 (16 bytes) || position (8 bytes, BE)`.
//!
//! The zero bytes are reserved. What a position means depends on the file: the byte offset of
//! the sealed unit in an SSTable, the byte offset of the entry in a value-log file, the logical
//! record index (from 1) in a WAL segment, and 0 for the manifest, which is sealed once per file
//! key.
//!
//! A (key, nonce) pair therefore cannot repeat, for three reasons that do not depend on each
//! other's details:
//!
//! 1. The file key is unique per writer: it is derived from a 256 bit salt drawn from the OS
//!    generator by the writer and from the file's identity, so two writers never share one.
//! 2. Within a key the nonce is injective in the position, since the position is written whole into
//!    a fixed-width field, and no other field of the nonce varies.
//! 3. [`FileCipher`] refuses to seal at a position that is not strictly after the last one it
//!    sealed, so one cipher never uses a position twice, even from several threads, and a seal that
//!    fails after claiming its position keeps the claim.
//!
//! Only a cipher made from a freshly generated key can seal, so a second cipher over the same
//! key, which would start a second sequence of positions, cannot exist.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use aes_gcm::aead::{Nonce, Tag};
use aes_gcm::{AeadInOut, Aes256Gcm, KeyInit};
use chacha20poly1305::XChaCha20Poly1305;

use super::identity::{FileIdentity, FileKind};
use super::kdf::{FileKey, FILE_KEY_LEN};
use super::suite::CipherSuite;
use crate::error::EncryptionError;

/// Length in bytes of the authentication tag every suite appends to a sealed block.
pub const TAG_LEN: usize = 16;

/// Length in bytes of the associated data every block is bound to.
pub const AAD_LEN: usize = 48;

/// Version of the associated-data layout.
const AAD_VERSION: u8 = 1;

/// The longest nonce of any suite.
const MAX_NONCE_LEN: usize = 24;

const TOO_LONG: EncryptionError = EncryptionError::InvalidInput("the block is too long to seal");
const INTERNAL: EncryptionError =
	EncryptionError::InvalidInput("the cipher could not be set up for this suite");

/// What a sealed block holds. Part of its authenticated data, so a block cannot be read as
/// another kind. Each kind belongs to exactly one kind of file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
#[repr(u8)]
pub enum BlockKind {
	/// A data block of an SSTable.
	SstData = 1,
	/// A partition of an SSTable's index.
	SstIndexPartition = 2,
	/// The top-level index of an SSTable.
	SstTopLevelIndex = 3,
	/// A partition of an SSTable's filter.
	SstFilterPartition = 4,
	/// The index of an SSTable's filter partitions.
	SstFilterIndex = 5,
	/// The meta-index block of an SSTable.
	SstMetaIndex = 6,
	/// One entry of a value-log file.
	VlogEntry = 7,
	/// One logical record of a WAL segment, a whole batch.
	WalBatch = 8,
	/// The body of the manifest.
	ManifestBody = 9,
}

impl BlockKind {
	/// The id the kind is bound under.
	pub const fn id(self) -> u8 {
		self as u8
	}

	/// The kind of file this kind of block lives in.
	pub const fn file_kind(self) -> FileKind {
		match self {
			Self::SstData
			| Self::SstIndexPartition
			| Self::SstTopLevelIndex
			| Self::SstFilterPartition
			| Self::SstFilterIndex
			| Self::SstMetaIndex => FileKind::Sst,
			Self::VlogEntry => FileKind::Vlog,
			Self::WalBatch => FileKind::Wal,
			Self::ManifestBody => FileKind::Manifest,
		}
	}
}

/// What a block is bound to besides its file: what it is, where it is, and how its bytes were
/// compressed before sealing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockContext {
	block_kind: BlockKind,
	position: u64,
	compression: u8,
}

impl BlockContext {
	/// The context of a block of `block_kind` at `position`, whose plaintext was compressed with
	/// the algorithm stored as `compression` (0 if the file has no such byte).
	///
	/// `position` is also the block's nonce slot, see the module documentation for what it is in
	/// each kind of file.
	pub const fn new(block_kind: BlockKind, position: u64, compression: u8) -> Self {
		Self {
			block_kind,
			position,
			compression,
		}
	}

	/// What the block holds.
	pub const fn block_kind(&self) -> BlockKind {
		self.block_kind
	}

	/// The block's position and nonce slot.
	pub const fn position(&self) -> u64 {
		self.position
	}

	/// The stored compression byte.
	pub const fn compression(&self) -> u8 {
		self.compression
	}

	/// The associated data of a block of the file `identity`, see the module documentation for
	/// the layout.
	pub fn aad(&self, identity: &FileIdentity) -> [u8; AAD_LEN] {
		let mut aad = [0u8; AAD_LEN];
		aad[0] = AAD_VERSION;
		aad[1] = identity.suite.id();
		aad[2] = identity.file_kind.id();
		aad[3] = self.block_kind.id();
		aad[4..8].copy_from_slice(&identity.key_id.to_be_bytes());
		aad[8..16].copy_from_slice(&identity.file_id.to_be_bytes());
		aad[16..32].copy_from_slice(identity.db_id.as_bytes());
		aad[32..40].copy_from_slice(&self.position.to_be_bytes());
		aad[40] = self.compression;
		aad
	}
}

/// The nonce of the block at `position` and how many of its bytes the suite uses.
///
/// The position goes into the last eight bytes, big-endian, and everything before is zero.
fn nonce_for(suite: CipherSuite, position: u64) -> ([u8; MAX_NONCE_LEN], usize) {
	let len = suite.nonce_len();
	let mut nonce = [0u8; MAX_NONCE_LEN];
	// Every suite's nonce is at least 12 bytes, so this cannot underflow.
	nonce[len - 8..len].copy_from_slice(&position.to_be_bytes());
	(nonce, len)
}

/// The AEAD of a suite with its key schedule built once.
enum Primitive {
	Aes(Box<Aes256Gcm>),
	XChaCha(Box<XChaCha20Poly1305>),
}

impl Primitive {
	fn new(suite: CipherSuite, key: &[u8; FILE_KEY_LEN]) -> Result<Self, EncryptionError> {
		let primitive = match suite {
			CipherSuite::Aes256Gcm => {
				Aes256Gcm::new_from_slice(key).map(|cipher| Self::Aes(Box::new(cipher)))
			}
			CipherSuite::XChaCha20Poly1305 => {
				XChaCha20Poly1305::new_from_slice(key).map(|cipher| Self::XChaCha(Box::new(cipher)))
			}
		};
		primitive.map_err(|_| INTERNAL)
	}

	/// Encrypts `data` in place and returns the tag. Fails only if `data` or `aad` is longer than
	/// the suite allows, and then leaves `data` as it was.
	fn seal(
		&self,
		nonce: &[u8],
		aad: &[u8],
		data: &mut [u8],
	) -> Result<[u8; TAG_LEN], EncryptionError> {
		let tag = match self {
			Self::Aes(cipher) => {
				let nonce = Nonce::<Aes256Gcm>::try_from(nonce).map_err(|_| INTERNAL)?;
				let tag = cipher
					.encrypt_inout_detached(&nonce, aad, data.into())
					.map_err(|_| TOO_LONG)?;
				<[u8; TAG_LEN]>::try_from(tag.as_slice())
			}
			Self::XChaCha(cipher) => {
				let nonce = Nonce::<XChaCha20Poly1305>::try_from(nonce).map_err(|_| INTERNAL)?;
				let tag = cipher
					.encrypt_inout_detached(&nonce, aad, data.into())
					.map_err(|_| TOO_LONG)?;
				<[u8; TAG_LEN]>::try_from(tag.as_slice())
			}
		};
		tag.map_err(|_| INTERNAL)
	}

	/// Verifies `tag` over `data` and `aad`, then decrypts `data` in place. Both suites check the
	/// tag before touching the bytes, so a failure leaves `data` as it was and nothing
	/// unauthenticated is ever exposed as plaintext.
	fn open(
		&self,
		nonce: &[u8],
		aad: &[u8],
		data: &mut [u8],
		tag: &[u8; TAG_LEN],
	) -> Result<(), EncryptionError> {
		match self {
			Self::Aes(cipher) => {
				let nonce = Nonce::<Aes256Gcm>::try_from(nonce).map_err(|_| INTERNAL)?;
				let tag = Tag::<Aes256Gcm>::try_from(tag.as_slice()).map_err(|_| INTERNAL)?;
				cipher
					.decrypt_inout_detached(&nonce, aad, data.into(), &tag)
					.map_err(|_| EncryptionError::AuthenticationFailed)
			}
			Self::XChaCha(cipher) => {
				let nonce = Nonce::<XChaCha20Poly1305>::try_from(nonce).map_err(|_| INTERNAL)?;
				let tag =
					Tag::<XChaCha20Poly1305>::try_from(tag.as_slice()).map_err(|_| INTERNAL)?;
				cipher
					.decrypt_inout_detached(&nonce, aad, data.into(), &tag)
					.map_err(|_| EncryptionError::AuthenticationFailed)
			}
		}
	}
}

/// The tag of a file header: an AEAD seal of no plaintext with `header` as the associated data,
/// under the key made for the header and the all-zero nonce of position 0.
///
/// The key is a different one from the file's data key and is used for this one message, so the
/// nonce can never repeat under it and never collides with a data block's.
pub(super) fn seal_header_tag(
	suite: CipherSuite,
	key: &[u8; FILE_KEY_LEN],
	header: &[u8],
) -> Result<[u8; TAG_LEN], EncryptionError> {
	let primitive = Primitive::new(suite, key)?;
	let (nonce, len) = nonce_for(suite, 0);
	primitive.seal(&nonce[..len], header, &mut [])
}

/// Checks a header tag made by [`seal_header_tag`]. A mismatch is `AuthenticationFailed`.
pub(super) fn open_header_tag(
	suite: CipherSuite,
	key: &[u8; FILE_KEY_LEN],
	header: &[u8],
	tag: &[u8; TAG_LEN],
) -> Result<(), EncryptionError> {
	let primitive = Primitive::new(suite, key)?;
	let (nonce, len) = nonce_for(suite, 0);
	primitive.open(&nonce[..len], header, &mut [], tag)
}

/// Seals and opens the blocks of one file. Built from a [`FileKey`], which it consumes.
///
/// The key schedule is built once, so it is cheap to keep for the life of a file and to share:
/// opening only reads it, and `FileCipher` is `Send + Sync`. Sealing is meant for the single
/// writer of the file; its position check is atomic, so even concurrent callers cannot seal the
/// same position twice, but only in-order callers will not be refused.
pub struct FileCipher {
	primitive: Primitive,
	identity: FileIdentity,
	can_seal: bool,
	/// The smallest position that may still be sealed. Starts at 0, so the first seal can use
	/// any position including 0, and moves to one past each position as it is sealed.
	next_position: AtomicU64,
}

impl fmt::Debug for FileCipher {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("FileCipher")
			.field("identity", &self.identity)
			.field("can_seal", &self.can_seal)
			.field("next_position", &self.next_position.load(Ordering::Relaxed))
			.finish_non_exhaustive()
	}
}

impl FileCipher {
	/// Builds the cipher of a file from its key, which is consumed and wiped.
	pub fn new(key: FileKey) -> Result<Self, EncryptionError> {
		let (identity, dek, can_seal) = key.into_parts();
		let primitive = Primitive::new(identity.suite, &dek)?;
		Ok(Self {
			primitive,
			identity,
			can_seal,
			next_position: AtomicU64::new(0),
		})
	}

	/// The identity of the file the cipher belongs to.
	pub const fn identity(&self) -> &FileIdentity {
		&self.identity
	}

	/// The suite the file is sealed with.
	pub const fn suite(&self) -> CipherSuite {
		self.identity.suite
	}

	/// Whether the cipher may seal. Only a cipher made from a freshly generated key may.
	pub const fn can_seal(&self) -> bool {
		self.can_seal
	}

	/// The smallest position that may still be sealed.
	pub fn next_position(&self) -> u64 {
		self.next_position.load(Ordering::Acquire)
	}

	/// Seals `data` in place and returns the tag, which the caller stores after the sealed
	/// bytes.
	///
	/// Fails with `PositionReuse` if `context.position()` is not strictly after the last
	/// position sealed with this cipher, and with `InvalidInput` for a cipher that can only
	/// open, a block kind that does not belong to the file, or position `u64::MAX`. A refused
	/// seal leaves `data` as it was.
	pub fn seal_detached(
		&self,
		context: &BlockContext,
		data: &mut [u8],
	) -> Result<[u8; TAG_LEN], EncryptionError> {
		if !self.can_seal {
			return Err(EncryptionError::InvalidInput(
				"a cipher made for an existing file cannot seal",
			));
		}
		self.check_kind(context)?;
		self.claim(context.position)?;
		let (nonce, len) = nonce_for(self.identity.suite, context.position);
		self.primitive.seal(&nonce[..len], &context.aad(&self.identity), data)
	}

	/// Seals `data` in place and appends the tag, giving `ciphertext || tag`.
	pub fn seal_append(
		&self,
		context: &BlockContext,
		data: &mut Vec<u8>,
	) -> Result<(), EncryptionError> {
		let tag = self.seal_detached(context, data.as_mut_slice())?;
		data.extend_from_slice(&tag);
		Ok(())
	}

	/// Verifies `tag` over `data` and decrypts `data` in place.
	///
	/// Fails with `AuthenticationFailed` if anything the block is bound to differs from what it
	/// was sealed with, or the bytes or the tag were changed, and then leaves `data` as it was.
	/// Nothing is decrypted, decompressed or parsed before this succeeds.
	pub fn open_detached(
		&self,
		context: &BlockContext,
		data: &mut [u8],
		tag: &[u8; TAG_LEN],
	) -> Result<(), EncryptionError> {
		self.check_kind(context)?;
		let (nonce, len) = nonce_for(self.identity.suite, context.position);
		self.primitive.open(&nonce[..len], &context.aad(&self.identity), data, tag)
	}

	/// Opens `ciphertext || tag` in place and returns the plaintext, which is the front of
	/// `sealed`. Input too short to hold a tag fails with `AuthenticationFailed`.
	pub fn open_suffix<'a>(
		&self,
		context: &BlockContext,
		sealed: &'a mut [u8],
	) -> Result<&'a mut [u8], EncryptionError> {
		let split =
			sealed.len().checked_sub(TAG_LEN).ok_or(EncryptionError::AuthenticationFailed)?;
		let (data, tag) = sealed.split_at_mut(split);
		let tag =
			<[u8; TAG_LEN]>::try_from(&*tag).map_err(|_| EncryptionError::AuthenticationFailed)?;
		self.open_detached(context, data, &tag)?;
		Ok(data)
	}

	fn check_kind(&self, context: &BlockContext) -> Result<(), EncryptionError> {
		if context.block_kind.file_kind() == self.identity.file_kind {
			Ok(())
		} else {
			Err(EncryptionError::InvalidInput(
				"the block kind does not belong to this kind of file",
			))
		}
	}

	/// Takes `position` for a seal, or refuses if it is not strictly after every position taken
	/// before. A compare-and-swap loop, so of several threads taking the same position exactly
	/// one gets it.
	fn claim(&self, position: u64) -> Result<(), EncryptionError> {
		let after = position
			.checked_add(1)
			.ok_or(EncryptionError::InvalidInput("the position is out of range"))?;
		let mut next = self.next_position.load(Ordering::Acquire);
		loop {
			if position < next {
				return Err(EncryptionError::PositionReuse);
			}
			match self.next_position.compare_exchange_weak(
				next,
				after,
				Ordering::AcqRel,
				Ordering::Acquire,
			) {
				Ok(_) => return Ok(()),
				Err(observed) => next = observed,
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashSet;

	use zeroize::Zeroizing;

	use super::*;
	use crate::tde::identity::DbId;
	use crate::tde::keys::{KeyManager, KeyState, ROOT_KEY_LEN};
	use crate::tde::random::SALT_LEN;
	use crate::tde::testutil::assert_leaks_no_bytes;
	use crate::tde::SoftwareKeyManager;

	const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

	const ROOT: [u8; ROOT_KEY_LEN] = [0x5A; ROOT_KEY_LEN];
	const SALT: [u8; SALT_LEN] = [0x3C; SALT_LEN];

	fn db_id() -> DbId {
		DbId::from_bytes(std::array::from_fn(|i| 0xA0 + i as u8))
	}

	fn identity(suite: CipherSuite, file_kind: FileKind) -> FileIdentity {
		FileIdentity::new(suite, file_kind, 7, 0x0102_0304_0506_0708, db_id())
	}

	/// A writer and a reader of the same file, with a fixed root and salt.
	fn pair(identity: FileIdentity) -> (FileCipher, FileCipher) {
		let writer =
			FileCipher::new(FileKey::derive_for_test(&ROOT, SALT, identity, true)).unwrap();
		let reader =
			FileCipher::new(FileKey::derive_for_test(&ROOT, SALT, identity, false)).unwrap();
		(writer, reader)
	}

	fn sst_data(position: u64) -> BlockContext {
		BlockContext::new(BlockKind::SstData, position, 1)
	}

	fn sealed(cipher: &FileCipher, context: &BlockContext, plaintext: &[u8]) -> Vec<u8> {
		let mut buf = plaintext.to_vec();
		cipher.seal_append(context, &mut buf).unwrap();
		buf
	}

	#[test]
	fn the_associated_data_has_the_documented_layout() {
		let identity = FileIdentity::new(
			CipherSuite::XChaCha20Poly1305,
			FileKind::Sst,
			0x0102_0304,
			0x1112_1314_1516_1718,
			db_id(),
		);
		let context = BlockContext::new(BlockKind::SstMetaIndex, 0x2122_2324_2526_2728, 0x31);

		let mut expected = vec![0x01, 0x02, 0x01, 0x06];
		expected.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);
		expected.extend_from_slice(&[0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18]);
		expected.extend_from_slice(&(0xA0u8..0xB0).collect::<Vec<_>>());
		expected.extend_from_slice(&[0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28]);
		expected.push(0x31);
		expected.extend_from_slice(&[0; 7]);
		assert_eq!(expected.len(), AAD_LEN);
		assert_eq!(context.aad(&identity).to_vec(), expected);
	}

	#[test]
	fn the_nonce_is_the_position_in_the_last_eight_bytes() {
		let position = 0x0102_0304_0506_0708;
		let (nonce, len) = nonce_for(CipherSuite::Aes256Gcm, position);
		assert_eq!(len, 12);
		assert_eq!(nonce[..len], [0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8]);

		let (nonce, len) = nonce_for(CipherSuite::XChaCha20Poly1305, position);
		assert_eq!(len, 24);
		let mut expected = [0u8; 24];
		expected[16..].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
		assert_eq!(nonce[..len], expected);

		// Position 0, the manifest's, is the all-zero nonce, and the largest position fills the
		// last eight bytes.
		assert_eq!(nonce_for(CipherSuite::Aes256Gcm, 0).0, [0u8; MAX_NONCE_LEN]);
		let (nonce, len) = nonce_for(CipherSuite::XChaCha20Poly1305, u64::MAX);
		assert_eq!(nonce[..16], [0u8; 16]);
		assert_eq!(nonce[16..len], [0xFF; 8]);
	}

	#[test]
	fn block_kinds_belong_to_exactly_one_kind_of_file() {
		let expected = [
			(BlockKind::SstData, 1, FileKind::Sst),
			(BlockKind::SstIndexPartition, 2, FileKind::Sst),
			(BlockKind::SstTopLevelIndex, 3, FileKind::Sst),
			(BlockKind::SstFilterPartition, 4, FileKind::Sst),
			(BlockKind::SstFilterIndex, 5, FileKind::Sst),
			(BlockKind::SstMetaIndex, 6, FileKind::Sst),
			(BlockKind::VlogEntry, 7, FileKind::Vlog),
			(BlockKind::WalBatch, 8, FileKind::Wal),
			(BlockKind::ManifestBody, 9, FileKind::Manifest),
		];
		for (kind, id, file_kind) in expected {
			assert_eq!(kind.id(), id);
			assert_eq!(kind.file_kind(), file_kind);
		}
	}

	#[test]
	fn blocks_round_trip_in_every_form_and_size() {
		for suite in SUITES {
			let (writer, reader) = pair(identity(suite, FileKind::Sst));
			for (i, len) in [0usize, 1, 15, 16, 17, 63, 64, 65, 200].into_iter().enumerate() {
				let plaintext: Vec<u8> = (0..len).map(|b| (b * 7 + i) as u8).collect();
				// Positions only move forward over the whole life of the writer.
				let base = 100 * i as u64;

				let context = sst_data(base + 10);
				let mut sealed = sealed(&writer, &context, &plaintext);
				assert_eq!(sealed.len(), len + TAG_LEN);
				// Eight or more bytes, so that a keystream that happens to match is not a risk.
				if len >= 8 {
					assert_ne!(
						sealed[..len],
						plaintext[..],
						"{suite:?}: sealed bytes are plaintext"
					);
				}
				let opened = reader.open_suffix(&context, &mut sealed).unwrap();
				assert_eq!(opened, &plaintext[..], "{suite:?} len {len}");

				// The detached form agrees with the appended one.
				let mut data = plaintext.clone();
				let context = sst_data(base + 20);
				let tag = writer.seal_detached(&context, &mut data).unwrap();
				reader.open_detached(&context, &mut data, &tag).unwrap();
				assert_eq!(data, plaintext);
			}
		}
	}

	#[test]
	fn the_same_plaintext_seals_differently_at_different_positions_and_under_different_suites() {
		let plaintext = b"the same sixteen bytes".to_vec();
		let (aes, _) = pair(identity(CipherSuite::Aes256Gcm, FileKind::Sst));
		let (xchacha, _) = pair(identity(CipherSuite::XChaCha20Poly1305, FileKind::Sst));
		let a = sealed(&aes, &sst_data(1), &plaintext);
		let b = sealed(&aes, &sst_data(2), &plaintext);
		let c = sealed(&xchacha, &sst_data(1), &plaintext);
		assert_ne!(a, b);
		assert_ne!(a, c);
	}

	#[test]
	fn a_block_fails_to_open_if_anything_it_is_bound_to_differs() {
		for suite in SUITES {
			let base = identity(suite, FileKind::Sst);
			let (writer, reader) = pair(base);
			let context = sst_data(4096);
			let plaintext = b"a block of the sorted string table";
			let sealed = sealed(&writer, &context, plaintext);

			// Control: the reader opens it.
			let mut control = sealed.clone();
			assert_eq!(reader.open_suffix(&context, &mut control).unwrap(), plaintext);

			let assert_rejected = |what: &str, cipher: &FileCipher, context: &BlockContext| {
				let mut copy = sealed.clone();
				let err = cipher.open_suffix(context, &mut copy).unwrap_err();
				assert_eq!(err, EncryptionError::AuthenticationFailed, "{suite:?}: {what}");
				assert_eq!(copy, sealed, "{suite:?}: {what} modified the data it rejected");
			};

			// What the context carries.
			assert_rejected("position", &reader, &sst_data(4097));
			assert_rejected("position by its top bit", &reader, &sst_data(4096 | (1 << 63)));
			assert_rejected(
				"block kind",
				&reader,
				&BlockContext::new(BlockKind::SstIndexPartition, 4096, 1),
			);
			assert_rejected(
				"compression byte",
				&reader,
				&BlockContext::new(BlockKind::SstData, 4096, 2),
			);

			// What the file's identity carries, and the key.
			let other_db = DbId::from_bytes(std::array::from_fn(|i| 0xB0 + i as u8));
			let variants = [
				(
					"file id",
					FileIdentity {
						file_id: base.file_id + 1,
						..base
					},
				),
				(
					"key id",
					FileIdentity {
						key_id: base.key_id + 1,
						..base
					},
				),
				(
					"db id",
					FileIdentity {
						db_id: other_db,
						..base
					},
				),
				(
					"file kind",
					FileIdentity {
						file_kind: FileKind::Vlog,
						..base
					},
				),
				(
					"suite",
					FileIdentity {
						suite: if suite == CipherSuite::Aes256Gcm {
							CipherSuite::XChaCha20Poly1305
						} else {
							CipherSuite::Aes256Gcm
						},
						..base
					},
				),
			];
			for (what, variant) in variants {
				let cipher =
					FileCipher::new(FileKey::derive_for_test(&ROOT, SALT, variant, false)).unwrap();
				// A block kind of another kind of file is a caller error, not an authentication
				// failure, so the vlog variant is asked with a vlog block kind.
				let context = if variant.file_kind == FileKind::Vlog {
					BlockContext::new(BlockKind::VlogEntry, 4096, 1)
				} else {
					context
				};
				assert_rejected(what, &cipher, &context);
			}
			let mut other_salt = SALT;
			other_salt[0] ^= 1;
			let cipher =
				FileCipher::new(FileKey::derive_for_test(&ROOT, other_salt, base, false)).unwrap();
			assert_rejected("salt", &cipher, &context);
			let mut other_root = ROOT;
			other_root[0] ^= 1;
			let cipher =
				FileCipher::new(FileKey::derive_for_test(&other_root, SALT, base, false)).unwrap();
			assert_rejected("root key", &cipher, &context);
		}
	}

	#[test]
	fn flipping_any_bit_of_a_sealed_block_fails_authentication() {
		for suite in SUITES {
			let (writer, reader) = pair(identity(suite, FileKind::Wal));
			let context = BlockContext::new(BlockKind::WalBatch, 9, 0);
			let sealed = sealed(&writer, &context, b"one batch of a write-ahead log segment");
			for byte in 0..sealed.len() {
				for bit in 0..8 {
					let mut copy = sealed.clone();
					copy[byte] ^= 1 << bit;
					let before = copy.clone();
					let err = reader.open_suffix(&context, &mut copy).unwrap_err();
					assert_eq!(
						err,
						EncryptionError::AuthenticationFailed,
						"{suite:?} {byte}.{bit}"
					);
					assert_eq!(copy, before, "{suite:?}: a rejected block was modified");
				}
			}
		}
	}

	#[test]
	fn input_too_short_to_hold_a_tag_fails_authentication_without_panicking() {
		for suite in SUITES {
			let (_, reader) = pair(identity(suite, FileKind::Sst));
			for len in 0..TAG_LEN {
				let mut short = vec![0xAB; len];
				let err = reader.open_suffix(&sst_data(1), &mut short).unwrap_err();
				assert_eq!(err, EncryptionError::AuthenticationFailed);
			}
			// Exactly a tag and no data is a well-formed message of zero bytes, just not an
			// authentic one.
			let mut empty = vec![0xAB; TAG_LEN];
			assert_eq!(
				reader.open_suffix(&sst_data(1), &mut empty).unwrap_err(),
				EncryptionError::AuthenticationFailed
			);
		}
	}

	#[test]
	fn an_empty_block_seals_to_a_tag_and_opens() {
		for suite in SUITES {
			let (writer, reader) = pair(identity(suite, FileKind::Manifest));
			let context = BlockContext::new(BlockKind::ManifestBody, 0, 0);
			let mut sealed = sealed(&writer, &context, b"");
			assert_eq!(sealed.len(), TAG_LEN);
			assert!(reader.open_suffix(&context, &mut sealed).unwrap().is_empty());
		}
	}

	#[test]
	fn a_position_can_only_move_forward() {
		for suite in SUITES {
			let (writer, _) = pair(identity(suite, FileKind::Wal));
			let at = |position| BlockContext::new(BlockKind::WalBatch, position, 0);
			let seal = |position| writer.seal_append(&at(position), &mut b"batch".to_vec());

			assert_eq!(writer.next_position(), 0);
			seal(1).unwrap();
			assert_eq!(writer.next_position(), 2);
			assert_eq!(seal(1).unwrap_err(), EncryptionError::PositionReuse);
			assert_eq!(seal(0).unwrap_err(), EncryptionError::PositionReuse);
			// Gaps are fine: positions are offsets or indexes, not a dense counter.
			seal(5).unwrap();
			assert_eq!(seal(4).unwrap_err(), EncryptionError::PositionReuse);
			assert_eq!(seal(5).unwrap_err(), EncryptionError::PositionReuse);
			seal(6).unwrap();
			assert_eq!(writer.next_position(), 7);
		}
	}

	#[test]
	fn a_refused_seal_leaves_the_data_and_the_position_alone() {
		let (writer, _) = pair(identity(CipherSuite::Aes256Gcm, FileKind::Wal));
		let at = |position| BlockContext::new(BlockKind::WalBatch, position, 0);
		writer.seal_append(&at(3), &mut b"first".to_vec()).unwrap();

		let mut data = b"second".to_vec();
		let err = writer.seal_append(&at(3), &mut data).unwrap_err();
		assert_eq!(err, EncryptionError::PositionReuse);
		assert_eq!(data, b"second");
		assert_eq!(writer.next_position(), 4);
	}

	#[test]
	fn the_first_position_may_be_zero() {
		for suite in SUITES {
			let (writer, reader) = pair(identity(suite, FileKind::Manifest));
			let context = BlockContext::new(BlockKind::ManifestBody, 0, 0);
			let mut sealed = sealed(&writer, &context, b"manifest body");
			assert_eq!(reader.open_suffix(&context, &mut sealed).unwrap(), b"manifest body");
			// A manifest is sealed once per key: a second seal at 0 is a reuse.
			let err = writer.seal_append(&context, &mut b"again".to_vec()).unwrap_err();
			assert_eq!(err, EncryptionError::PositionReuse);
		}
	}

	#[test]
	fn the_largest_position_is_refused_instead_of_overflowing() {
		let (writer, _) = pair(identity(CipherSuite::Aes256Gcm, FileKind::Wal));
		let at = |position| BlockContext::new(BlockKind::WalBatch, position, 0);
		let err = writer.seal_append(&at(u64::MAX), &mut b"x".to_vec()).unwrap_err();
		assert!(matches!(err, EncryptionError::InvalidInput(_)), "{err:?}");
		assert_eq!(writer.next_position(), 0);
		writer.seal_append(&at(u64::MAX - 1), &mut b"x".to_vec()).unwrap();
		assert_eq!(writer.next_position(), u64::MAX);
	}

	#[test]
	fn a_block_kind_of_another_kind_of_file_is_refused_and_keeps_the_position() {
		for suite in SUITES {
			let (writer, reader) = pair(identity(suite, FileKind::Sst));
			let wal = BlockContext::new(BlockKind::WalBatch, 10, 0);
			let mut data = b"data".to_vec();
			let err = writer.seal_append(&wal, &mut data).unwrap_err();
			assert!(matches!(err, EncryptionError::InvalidInput(_)), "{err:?}");
			assert_eq!(data, b"data");
			assert_eq!(writer.next_position(), 0);

			let mut sealed = vec![0u8; 32];
			let err = reader.open_suffix(&wal, &mut sealed).unwrap_err();
			assert!(matches!(err, EncryptionError::InvalidInput(_)), "{err:?}");
		}
	}

	#[test]
	fn a_cipher_for_an_existing_file_opens_but_cannot_seal() {
		for suite in SUITES {
			let (writer, reader) = pair(identity(suite, FileKind::Sst));
			assert!(writer.can_seal());
			assert!(!reader.can_seal());

			let context = sst_data(64);
			let mut data = b"data".to_vec();
			let err = reader.seal_append(&context, &mut data).unwrap_err();
			assert!(matches!(err, EncryptionError::InvalidInput(_)), "{err:?}");
			assert_eq!(data, b"data");
			assert_eq!(reader.next_position(), 0);

			let mut sealed = sealed(&writer, &context, b"data");
			assert_eq!(reader.open_suffix(&context, &mut sealed).unwrap(), b"data");
		}
	}

	/// A manager that returns the same root for every key id.
	struct ConstantRoot;

	impl KeyManager for ConstantRoot {
		fn active_key_id(&self) -> u32 {
			1
		}

		fn key_state(&self, _key_id: u32) -> KeyState {
			KeyState::Active
		}

		fn root_key(&self, _key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
			Ok(Zeroizing::new([0x42; ROOT_KEY_LEN]))
		}
	}

	#[test]
	fn two_files_of_a_constant_root_manager_do_not_share_keys_or_ciphertext() {
		for suite in SUITES {
			let km = ConstantRoot;
			let plaintext = b"identical plaintext in two identical files";
			let context = sst_data(128);
			let mut outputs = HashSet::new();
			for _ in 0..3 {
				// The same database, kind and id every time: nothing but the engine's salt differs.
				let key = FileKey::generate(&km, db_id(), suite, FileKind::Sst, 77).unwrap();
				let (identity, salt) = (key.identity(), *key.salt());
				let writer = FileCipher::new(key).unwrap();
				let sealed = sealed(&writer, &context, plaintext);
				assert!(outputs.insert(sealed.clone()), "{suite:?}: two files sealed identically");

				// Each file opens with its own salt, and the others' ciphertext does not open.
				let reader =
					FileCipher::new(FileKey::for_reading(&km, identity, salt).unwrap()).unwrap();
				let mut copy = sealed.clone();
				assert_eq!(reader.open_suffix(&context, &mut copy).unwrap(), plaintext);
				for other in outputs.iter().filter(|other| **other != sealed) {
					let mut copy = other.clone();
					assert_eq!(
						reader.open_suffix(&context, &mut copy).unwrap_err(),
						EncryptionError::AuthenticationFailed
					);
				}
			}
		}
	}

	#[test]
	fn a_writer_and_a_reader_built_through_the_key_manager_agree() {
		for suite in SUITES {
			let km = SoftwareKeyManager::new([0x11; ROOT_KEY_LEN]);
			let key = FileKey::generate(&km, db_id(), suite, FileKind::Vlog, 4).unwrap();
			let (identity, salt) = (key.identity(), *key.salt());
			let writer = FileCipher::new(key).unwrap();
			let reader =
				FileCipher::new(FileKey::for_reading(&km, identity, salt).unwrap()).unwrap();
			assert_eq!(writer.identity(), reader.identity());
			assert_eq!(writer.suite(), suite);

			let context = BlockContext::new(BlockKind::VlogEntry, 25, 0);
			let mut sealed = sealed(&writer, &context, b"key and value of a vlog entry");
			assert_eq!(
				reader.open_suffix(&context, &mut sealed).unwrap(),
				b"key and value of a vlog entry"
			);
		}
	}

	fn unhex(text: &str) -> Vec<u8> {
		let digits: Vec<u8> = text.bytes().filter(u8::is_ascii_hexdigit).collect();
		assert_eq!(digits.len() % 2, 0, "odd number of hex digits");
		digits
			.chunks(2)
			.map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
			.collect()
	}

	fn hex(bytes: &[u8]) -> String {
		bytes.iter().map(|b| format!("{b:02x}")).collect()
	}

	/// Seals `plaintext` with the raw AEAD of `suite` under `key`, nonce and aad given, and
	/// checks that it opens again.
	fn seal_with_raw_aead(
		suite: CipherSuite,
		key: &[u8],
		nonce: &[u8],
		aad: &[u8],
		plaintext: &[u8],
	) -> (Vec<u8>, [u8; TAG_LEN]) {
		let primitive = Primitive::new(suite, key.try_into().unwrap()).unwrap();
		let mut data = plaintext.to_vec();
		let tag = primitive.seal(nonce, aad, &mut data).unwrap();
		let mut opened = data.clone();
		primitive.open(nonce, aad, &mut opened, &tag).unwrap();
		assert_eq!(opened, plaintext);
		(data, tag)
	}

	// NIST CAVP `gcmEncryptExtIV256.rsp` (the vector with a 16 byte plaintext and a 16 byte
	// additional data input that opens the set), as copied into the test data of the `aes-gcm`
	// crate. A published vector for the primitive, checked through the code path that seals
	// every block.
	#[test]
	fn aes_256_gcm_matches_a_published_nist_vector() {
		let key = unhex("92e11dcdaa866f5ce790fd24501f92509aacf4cb8b1339d50c9c1240935dd08b");
		let nonce = unhex("ac93a1a6145299bde902f21a");
		let aad = unhex("1e0889016f67601c8ebea4943bc23ad6");
		let plaintext = unhex("2d71bcfa914e4ac045b2aa60955fad24");
		let (ciphertext, tag) =
			seal_with_raw_aead(CipherSuite::Aes256Gcm, &key, &nonce, &aad, &plaintext);
		assert_eq!(hex(&ciphertext), "8995ae2e6df3dbf96fac7b7137bae67f");
		assert_eq!(hex(&tag), "eca5aa77d51d4a0a14d9c51e1da474ab");
	}

	// draft-irtf-cfrg-xchacha, appendix A.3.1 (the same key, associated data and plaintext as
	// RFC 8439 section 2.8.2, with a 24 byte nonce).
	#[test]
	fn xchacha20_poly1305_matches_the_published_ietf_draft_vector() {
		let key: Vec<u8> = (0x80..0xA0).collect();
		let nonce: Vec<u8> = (0x40..0x58).collect();
		let aad = unhex("50515253c0c1c2c3c4c5c6c7");
		let plaintext = b"Ladies and Gentlemen of the class of '99: If I could offer you only one \
			tip for the future, sunscreen would be it.";
		let (ciphertext, tag) =
			seal_with_raw_aead(CipherSuite::XChaCha20Poly1305, &key, &nonce, &aad, plaintext);
		assert_eq!(
			hex(&ciphertext),
			"bd6d179d3e83d43b9576579493c0e939572a1700252bfaccbed2902c21396cbb731c7f1b0b4aa644\
			0bf3a82f4eda7e39ae64c6708c54c216cb96b72e1213b4522f8c9ba40db5d945b11b69b982c1bb9e\
			3f3fac2bc369488f76b2383565d3fff921f9664c97637da9768812f615c68b13b52e"
		);
		assert_eq!(hex(&tag), "c0875924c1c7987947deafd8780acf49");
	}

	// Independently computed with the pure-Python AEADs that reproduce the two vectors above:
	// the file key from HKDF-SHA256 (root bytes 00..1f, salt bytes 20..3f, database id a0..af,
	// see the vectors in `kdf`), the associated data and nonce written out from the layouts in
	// this module's documentation, and the seal of the 32 plaintext bytes 40..5f (or none).
	#[test]
	fn sealed_blocks_match_independently_computed_vectors() {
		let root: [u8; ROOT_KEY_LEN] = std::array::from_fn(|i| i as u8);
		let salt: [u8; SALT_LEN] = std::array::from_fn(|i| 0x20 + i as u8);
		let plaintext: Vec<u8> = (0x40..0x60).collect();
		let sst = FileIdentity::new(
			CipherSuite::Aes256Gcm,
			FileKind::Sst,
			7,
			0x0102_0304_0506_0708,
			db_id(),
		);
		let wal = FileIdentity::new(
			CipherSuite::XChaCha20Poly1305,
			FileKind::Wal,
			0x0102_0304,
			0x1112_1314_1516_1718,
			db_id(),
		);
		let manifest = FileIdentity {
			file_kind: FileKind::Manifest,
			..sst
		};

		// (identity, context, plaintext, file key, aad, nonce, ciphertext, tag)
		let vectors = [
			(
				sst,
				BlockContext::new(BlockKind::SstData, 0x1000, 1),
				&plaintext[..],
				"d3a5ed5726eeb620450fb06ddc47108429797b14203abf18727421ddd1af4064",
				"01010101000000070102030405060708a0a1a2a3a4a5a6a7a8a9aaabacadaeaf\
				 00000000000010000100000000000000",
				"000000000000000000001000",
				"689bb5e65e684ca8e9f66d485f4578df8a309d40f13be3b11eef275d4ecf1a97",
				"33c922ca3c9525daa476151a4bf11efe",
			),
			(
				wal,
				BlockContext::new(BlockKind::WalBatch, 1, 0),
				&plaintext[..],
				"012d010e7f0155632efd20c9a6db17348ddef98976eda0e9a2f67b2b04a612bb",
				"01020308010203041112131415161718a0a1a2a3a4a5a6a7a8a9aaabacadaeaf\
				 00000000000000010000000000000000",
				"000000000000000000000000000000000000000000000001",
				"5bb6247108fddfb324a3e36347e1d3eaca125fbbfda3ec5588460defc909a98d",
				"d1ab67534b5ffd12ce98064f04650100",
			),
			(
				manifest,
				BlockContext::new(BlockKind::ManifestBody, 0, 0),
				&[],
				"18c40e412b1550c3bf42bbd3399f0adc204fac609b972f61c3498b96a8d1cbc6",
				"01010409000000070102030405060708a0a1a2a3a4a5a6a7a8a9aaabacadaeaf\
				 00000000000000000000000000000000",
				"000000000000000000000000",
				"",
				"1b6c7365fa2f8313296b095b8b8ead40",
			),
		];
		for (i, (identity, context, plaintext, dek, aad, nonce, ciphertext, tag)) in
			vectors.into_iter().enumerate()
		{
			let key = FileKey::derive_for_test(&root, salt, identity, true);
			assert_eq!(hex(&key.dek_for_test()), dek, "vector {i}: file key");
			assert_eq!(hex(&context.aad(&identity)), aad, "vector {i}: associated data");
			let (bytes, len) = nonce_for(identity.suite, context.position());
			assert_eq!(hex(&bytes[..len]), nonce, "vector {i}: nonce");

			let cipher = FileCipher::new(key).unwrap();
			let mut sealed = plaintext.to_vec();
			cipher.seal_append(&context, &mut sealed).unwrap();
			assert_eq!(hex(&sealed), format!("{ciphertext}{tag}"), "vector {i}: sealed block");

			// And back, through a reader that only derives its key from the header's contents.
			let reader =
				FileCipher::new(FileKey::derive_for_test(&root, salt, identity, false)).unwrap();
			assert_eq!(reader.open_suffix(&context, &mut sealed).unwrap(), plaintext, "vector {i}");
		}
	}

	#[test]
	fn ciphers_and_keys_can_be_shared_between_threads() {
		fn assert_send_sync<T: Send + Sync>() {}
		assert_send_sync::<FileCipher>();
		assert_send_sync::<FileKey>();
		assert_send_sync::<BlockContext>();
	}

	#[test]
	fn a_cipher_prints_no_key_material() {
		for suite in SUITES {
			let key = FileKey::derive_for_test(&ROOT, SALT, identity(suite, FileKind::Sst), true);
			let dek = key.dek_for_test();
			let cipher = FileCipher::new(key).unwrap();
			for text in [format!("{cipher:?}"), format!("{cipher:#?}")] {
				assert_leaks_no_bytes(&text, &[&dek, &ROOT, &SALT]);
				assert!(text.contains("FileCipher"), "{text}");
			}
		}
	}
}
