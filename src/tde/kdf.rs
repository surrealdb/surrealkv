//! The key of one file, derived from a root key, a fresh salt and the identity of the file.
//!
//! No key manager decides which key a file is sealed under: the engine always derives it, so a
//! manager cannot make two files share one, even by returning the same root for every id. A file
//! key is
//!
//! ```text
//! HKDF-SHA256(ikm = root, salt = salt32, info = LABEL || suite(1) || file_kind(1)
//!                                              || key_id(4, BE) || file_id(8, BE) || db_id(16))
//! ```
//!
//! with `LABEL = "surrealkv/tde/v1/dek"`, expanded to 32 bytes. The salt is drawn from the OS
//! random number generator for every writer, so two files never share a key unless the
//! generator fails.
//!
//! The tag of the file header is made with a second key from the same extraction, whose label is
//! `"surrealkv/tde/v1/hdr"` and whose other inputs are the same. It is a different key, so the
//! header's tag never uses one of the data key's nonces, and it is used for exactly one message.

use std::fmt;

use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

use super::identity::{DbId, FileIdentity, FileKind, DB_ID_LEN};
use super::keys::{KeyManager, KeyState, ROOT_KEY_LEN};
use super::random::{fresh_salt, SALT_LEN};
use super::suite::CipherSuite;
use crate::error::{EncryptionError, KeyProviderFailure};

/// Length in bytes of a file key.
pub const FILE_KEY_LEN: usize = 32;

/// Context label of the key that seals a file's data.
const DEK_LABEL: &[u8] = b"surrealkv/tde/v1/dek";

/// Context label of the key that authenticates a file's header. Same length as `DEK_LABEL`.
const HDR_LABEL: &[u8] = b"surrealkv/tde/v1/hdr";

/// Length of the `info` input: the label, suite, file kind, key id, file id and database id.
const INFO_LEN: usize = DEK_LABEL.len() + 1 + 1 + 4 + 8 + DB_ID_LEN;

/// How often the active key is looked up again when a rotation moves it between two questions.
const ACTIVE_KEY_ATTEMPTS: usize = 4;

/// The key of one file, and the identity and salt it was derived for.
///
/// Single-use on purpose: it is not `Clone` and is consumed by `FileCipher::new`. Only a key
/// made by [`generate`](Self::generate), which draws the salt itself, can be used to seal; a
/// key rebuilt from an existing file's header can only open, because sealing with it would
/// start a new nonce sequence under a key that has already been used.
///
/// It prints no key material.
pub struct FileKey {
	identity: FileIdentity,
	salt: [u8; SALT_LEN],
	dek: Zeroizing<[u8; FILE_KEY_LEN]>,
	/// The key that authenticates the file header, `None` once it has been used: it makes or
	/// checks exactly one tag.
	header_key: Option<Zeroizing<[u8; FILE_KEY_LEN]>>,
	can_seal: bool,
}

impl fmt::Debug for FileKey {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("FileKey")
			.field("identity", &self.identity)
			.field("can_seal", &self.can_seal)
			.finish_non_exhaustive()
	}
}

impl FileKey {
	/// The key for a file that is about to be written, under the manager's active key.
	///
	/// Draws a fresh salt from the OS random number generator, which is why a key made here can
	/// seal. Fails with `KeyRetired` if the active key is not in the active state, with
	/// `UnknownKeyId` if the manager does not know it, with whatever `root_key` fails with, and
	/// with `KeyProvider(RandomSource)` if no salt could be drawn. If the active key keeps
	/// changing while it is looked up, which only a manager that rotates continuously does, it
	/// fails with the retryable `KeyProvider(Unavailable)`.
	pub fn generate(
		key_manager: &dyn KeyManager,
		db_id: DbId,
		suite: CipherSuite,
		file_kind: FileKind,
		file_id: u64,
	) -> Result<Self, EncryptionError> {
		let (key_id, root) = active_root(key_manager)?;
		let salt = fresh_salt()?;
		let identity = FileIdentity::new(suite, file_kind, key_id, file_id, db_id);
		Self::derive(&root, salt, identity, true)
	}

	/// The key of an existing file, from the identity and salt its header records, which can
	/// open the file but not seal anything.
	///
	/// Fails with `UnknownKeyId` or `KeyRetired` if the key the file names cannot be used to
	/// read, with `MalformedHeader` for an all-zero salt, and with whatever `root_key` fails
	/// with. A key that was derived from a header that does not belong to the file is not
	/// detected here: that is what authenticating the header is for.
	pub fn for_reading(
		key_manager: &dyn KeyManager,
		identity: FileIdentity,
		salt: [u8; SALT_LEN],
	) -> Result<Self, EncryptionError> {
		if salt.iter().all(|&byte| byte == 0) {
			return Err(EncryptionError::MalformedHeader);
		}
		match key_manager.key_state(identity.key_id) {
			KeyState::Active | KeyState::DecryptOnly => {}
			KeyState::Retired => return Err(EncryptionError::KeyRetired),
			KeyState::Unknown => return Err(EncryptionError::UnknownKeyId),
		}
		let root = key_manager.root_key(identity.key_id)?;
		Self::derive(&root, salt, identity, false)
	}

	/// The identity the key was derived for.
	pub const fn identity(&self) -> FileIdentity {
		self.identity
	}

	/// The salt the key was derived with, which goes into the file header.
	pub const fn salt(&self) -> &[u8; SALT_LEN] {
		&self.salt
	}

	/// Whether a cipher made from this key may seal.
	pub const fn can_seal(&self) -> bool {
		self.can_seal
	}

	/// Splits the key for `FileCipher::new`, which is the only thing allowed to use it.
	pub(super) fn into_parts(self) -> (FileIdentity, Zeroizing<[u8; FILE_KEY_LEN]>, bool) {
		(self.identity, self.dek, self.can_seal)
	}

	/// Hands out the key that authenticates the file header, once. A second request fails with
	/// `PositionReuse`: the header has a single nonce slot, so a second message under the same key
	/// would repeat the nonce and break the authenticity of both tags.
	pub(super) fn take_header_key(
		&mut self,
	) -> Result<Zeroizing<[u8; FILE_KEY_LEN]>, EncryptionError> {
		self.header_key.take().ok_or(EncryptionError::PositionReuse)
	}

	fn derive(
		root: &[u8; ROOT_KEY_LEN],
		salt: [u8; SALT_LEN],
		identity: FileIdentity,
		can_seal: bool,
	) -> Result<Self, EncryptionError> {
		let hkdf = Hkdf::<Sha256>::new(Some(&salt), root);
		let dek = expand(&hkdf, DEK_LABEL, &identity)?;
		let header_key = expand(&hkdf, HDR_LABEL, &identity)?;
		Ok(Self {
			identity,
			salt,
			dek,
			header_key: Some(header_key),
			can_seal,
		})
	}

	/// The key derived from an explicit root and salt, so tests can compare against vectors.
	#[cfg(test)]
	pub(crate) fn derive_for_test(
		root: &[u8; ROOT_KEY_LEN],
		salt: [u8; SALT_LEN],
		identity: FileIdentity,
		can_seal: bool,
	) -> Self {
		Self::derive(root, salt, identity, can_seal).unwrap()
	}

	/// A copy of the header key, which is gone once it has been used.
	#[cfg(test)]
	pub(crate) fn header_key_for_test(&self) -> Option<[u8; FILE_KEY_LEN]> {
		self.header_key.as_ref().map(|key| **key)
	}

	/// A copy of the file key, for tests that compare keys or look for them in output.
	#[cfg(test)]
	pub(crate) fn dek_for_test(&self) -> [u8; FILE_KEY_LEN] {
		*self.dek
	}
}

/// The active key id and its root, read from the manager.
///
/// The manager answers `active_key_id`, `key_state` and `root_key` separately, so a rotation can
/// land between them: the id that was active a moment ago is decrypt-only by the time its state is
/// read. That is not a retired key, and failing the commit that happened to start a file at that
/// moment would turn an ordinary rotation into an error that is not retryable. A refusal is
/// therefore taken at its word only if the active key is still the one that was asked about;
/// otherwise the lookup starts again, a few times.
fn active_root(
	key_manager: &dyn KeyManager,
) -> Result<(u32, Zeroizing<[u8; ROOT_KEY_LEN]>), EncryptionError> {
	for _ in 0..ACTIVE_KEY_ATTEMPTS {
		let key_id = key_manager.active_key_id();
		let outcome = match key_manager.key_state(key_id) {
			KeyState::Active => key_manager.root_key(key_id),
			KeyState::DecryptOnly | KeyState::Retired => Err(EncryptionError::KeyRetired),
			KeyState::Unknown => Err(EncryptionError::UnknownKeyId),
		};
		match outcome {
			Ok(root) => return Ok((key_id, root)),
			Err(EncryptionError::KeyRetired | EncryptionError::UnknownKeyId)
				if key_manager.active_key_id() != key_id => {}
			Err(err) => return Err(err),
		}
	}
	Err(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable))
}

/// One 32 byte key out of the extraction, for `label`.
fn expand(
	hkdf: &Hkdf<Sha256>,
	label: &[u8],
	identity: &FileIdentity,
) -> Result<Zeroizing<[u8; FILE_KEY_LEN]>, EncryptionError> {
	let mut key = Zeroizing::new([0u8; FILE_KEY_LEN]);
	// Expanding to 32 bytes is far below the 255 blocks HKDF allows, so this does not fail; it is
	// an error rather than a panic because nothing derived from a failed expansion may be used.
	hkdf.expand(&info(label, identity), &mut *key)
		.map_err(|_| EncryptionError::InvalidInput("the file key could not be derived"))?;
	Ok(key)
}

/// The HKDF `info` input: `label || suite(1) || file_kind(1) || key_id(4, BE) || file_id(8, BE) ||
/// db_id(16)`.
fn info(label: &[u8], identity: &FileIdentity) -> Vec<u8> {
	let mut info = Vec::with_capacity(INFO_LEN);
	info.extend_from_slice(label);
	info.push(identity.suite.id());
	info.push(identity.file_kind.id());
	info.extend_from_slice(&identity.key_id.to_be_bytes());
	info.extend_from_slice(&identity.file_id.to_be_bytes());
	info.extend_from_slice(identity.db_id.as_bytes());
	info
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;
	use std::sync::atomic::{AtomicU32, Ordering};

	use super::*;
	use crate::tde::testutil::assert_leaks_no_bytes;
	use crate::tde::SoftwareKeyManager;

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

	fn hex(bytes: &[u8]) -> String {
		bytes.iter().map(|b| format!("{b:02x}")).collect()
	}

	#[test]
	fn the_info_input_is_the_label_then_the_identity_in_big_endian() {
		// Written out byte by byte, independently of `info`: the label, the suite, the file
		// kind, the key id, the file id and the database id.
		let mut expected = b"surrealkv/tde/v1/dek".to_vec();
		expected.extend_from_slice(&[0x01, 0x01]);
		expected.extend_from_slice(&[0x00, 0x00, 0x00, 0x07]);
		expected.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
		expected.extend_from_slice(&(0xA0u8..0xB0).collect::<Vec<_>>());
		assert_eq!(info(DEK_LABEL, &identity_a()), expected);
		assert_eq!(expected.len(), INFO_LEN);
		assert_eq!(INFO_LEN, 50);
	}

	// Expected values from an independent HKDF-SHA256 written with Python's `hmac` and
	// `hashlib` (checked against RFC 5869 test case 1): `PRK = HMAC(salt, root)`,
	// `OKM = HMAC(PRK, info || 0x01)`, with root bytes 00..1f, salt bytes 20..3f and database
	// id bytes a0..af.
	#[test]
	fn the_file_key_matches_independently_computed_hkdf_vectors() {
		let a = FileKey::derive_for_test(&ROOT, SALT, identity_a(), true);
		assert_eq!(
			hex(&a.dek_for_test()),
			"d3a5ed5726eeb620450fb06ddc47108429797b14203abf18727421ddd1af4064"
		);
		let b = FileKey::derive_for_test(&ROOT, SALT, identity_b(), true);
		assert_eq!(
			hex(&b.dek_for_test()),
			"012d010e7f0155632efd20c9a6db17348ddef98976eda0e9a2f67b2b04a612bb"
		);
	}

	// The same computation with the label "surrealkv/tde/v1/hdr", for the key that authenticates
	// the file header.
	#[test]
	fn the_header_key_matches_independently_computed_hkdf_vectors() {
		let a = FileKey::derive_for_test(&ROOT, SALT, identity_a(), true);
		assert_eq!(
			hex(&a.header_key_for_test().unwrap()),
			"907fef4f9da88c4a503b41582c6fc6533cf6429d2dd29295a17336e78a5c4be0"
		);
		let b = FileKey::derive_for_test(&ROOT, SALT, identity_b(), true);
		assert_eq!(
			hex(&b.header_key_for_test().unwrap()),
			"3b43c6f8527f85b8653cf74624cd135e33bb8230cd20149d7e5e36eea174f297"
		);
		// A different key from the data key, whatever the inputs.
		assert_ne!(a.header_key_for_test().unwrap(), a.dek_for_test());
		assert_ne!(b.header_key_for_test().unwrap(), b.dek_for_test());
	}

	#[test]
	fn the_header_key_can_be_taken_once() {
		let mut key = FileKey::derive_for_test(&ROOT, SALT, identity_a(), true);
		let expected = key.header_key_for_test().unwrap();
		assert_eq!(*key.take_header_key().unwrap(), expected);
		assert!(key.header_key_for_test().is_none());
		assert_eq!(key.take_header_key().unwrap_err(), EncryptionError::PositionReuse);
		// The data key is not affected.
		assert_eq!(
			key.dek_for_test(),
			FileKey::derive_for_test(&ROOT, SALT, identity_a(), true).dek_for_test()
		);
	}

	#[test]
	fn every_field_of_the_identity_and_the_salt_and_the_root_changes_the_key() {
		let base = FileKey::derive_for_test(&ROOT, SALT, identity_a(), true).dek_for_test();
		let id = identity_a();
		let mut other_db = *id.db_id.as_bytes();
		other_db[15] ^= 1;
		let variants = [
			FileIdentity {
				suite: CipherSuite::XChaCha20Poly1305,
				..id
			},
			FileIdentity {
				file_kind: FileKind::Vlog,
				..id
			},
			FileIdentity {
				key_id: 8,
				..id
			},
			FileIdentity {
				file_id: id.file_id + 1,
				..id
			},
			FileIdentity {
				db_id: DbId::from_bytes(other_db),
				..id
			},
		];
		for (i, variant) in variants.into_iter().enumerate() {
			let derived = FileKey::derive_for_test(&ROOT, SALT, variant, true).dek_for_test();
			assert_ne!(derived, base, "identity variant {i} left the key unchanged");
		}
		let mut salt = SALT;
		salt[0] ^= 1;
		assert_ne!(FileKey::derive_for_test(&ROOT, salt, id, true).dek_for_test(), base);
		let mut root = ROOT;
		root[31] ^= 1;
		assert_ne!(FileKey::derive_for_test(&root, SALT, id, true).dek_for_test(), base);
		// Whether the key may seal is not an input.
		assert_eq!(FileKey::derive_for_test(&ROOT, SALT, id, false).dek_for_test(), base);
	}

	/// A manager that is as hostile as the trait allows: one root for every key id, and
	/// nothing about the files it is asked for.
	struct ConstantRoot {
		active: u32,
	}

	impl KeyManager for ConstantRoot {
		fn active_key_id(&self) -> u32 {
			self.active
		}

		fn key_state(&self, _key_id: u32) -> KeyState {
			KeyState::Active
		}

		fn root_key(&self, _key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
			Ok(Zeroizing::new([0x42; ROOT_KEY_LEN]))
		}
	}

	#[test]
	fn a_manager_returning_a_constant_root_still_yields_distinct_file_keys() {
		let km = ConstantRoot {
			active: 1,
		};
		let mut seen = HashMap::new();
		for round in 0..4 {
			// The same file identity every time: only the salt the engine draws can differ.
			let key =
				FileKey::generate(&km, db_id(), CipherSuite::Aes256Gcm, FileKind::Sst, 9).unwrap();
			let previous = seen.insert(key.dek_for_test(), *key.salt());
			assert!(previous.is_none(), "round {round} repeated a file key");
		}
		// A second key id under the same constant root gives yet another key, even for the same
		// salt, because the key id is part of the derivation.
		let one = FileKey::derive_for_test(
			&[0x42; ROOT_KEY_LEN],
			SALT,
			FileIdentity::new(CipherSuite::Aes256Gcm, FileKind::Sst, 1, 9, db_id()),
			true,
		);
		let two = FileKey::derive_for_test(
			&[0x42; ROOT_KEY_LEN],
			SALT,
			FileIdentity::new(CipherSuite::Aes256Gcm, FileKind::Sst, 2, 9, db_id()),
			true,
		);
		assert_ne!(one.dek_for_test(), two.dek_for_test());
	}

	#[test]
	fn generate_draws_a_fresh_non_zero_salt_each_time() {
		let km = SoftwareKeyManager::new([0x11; ROOT_KEY_LEN]);
		let first =
			FileKey::generate(&km, db_id(), CipherSuite::Aes256Gcm, FileKind::Wal, 3).unwrap();
		let second =
			FileKey::generate(&km, db_id(), CipherSuite::Aes256Gcm, FileKind::Wal, 3).unwrap();
		assert_ne!(first.salt(), second.salt());
		assert_ne!(first.salt(), &[0u8; SALT_LEN]);
		assert!(first.can_seal() && second.can_seal());
		assert_eq!(first.identity().key_id, 1);
		assert_eq!(first.identity().file_id, 3);
		assert_eq!(first.identity().db_id, db_id());
	}

	#[test]
	fn generate_only_writes_under_an_active_key() {
		let km = SoftwareKeyManager::new([0x11; ROOT_KEY_LEN]);
		km.add_key(2, [0x22; ROOT_KEY_LEN]).unwrap();
		let generate = |km: &dyn KeyManager| {
			FileKey::generate(km, db_id(), CipherSuite::Aes256Gcm, FileKind::Sst, 1)
				.map(|key| key.identity().key_id)
		};
		assert_eq!(generate(&km).unwrap(), 1);
		km.set_active_key_id(2).unwrap();
		assert_eq!(generate(&km).unwrap(), 2);

		/// A manager whose active key id is in the given state, whatever the id.
		struct Stuck(KeyState);
		impl KeyManager for Stuck {
			fn active_key_id(&self) -> u32 {
				1
			}

			fn key_state(&self, _key_id: u32) -> KeyState {
				self.0
			}

			fn root_key(
				&self,
				_key_id: u32,
			) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
				Ok(Zeroizing::new([0x55; ROOT_KEY_LEN]))
			}
		}
		assert_eq!(
			generate(&Stuck(KeyState::DecryptOnly)).unwrap_err(),
			EncryptionError::KeyRetired
		);
		assert_eq!(generate(&Stuck(KeyState::Retired)).unwrap_err(), EncryptionError::KeyRetired);
		assert_eq!(generate(&Stuck(KeyState::Unknown)).unwrap_err(), EncryptionError::UnknownKeyId);
	}

	/// A manager that is rotated while a file is being started: the first time it is asked which
	/// key is active it says key 1, every later time key 2, and from then on key 1 is decrypt-only.
	struct RotatedMidLookup {
		asked: AtomicU32,
	}

	impl KeyManager for RotatedMidLookup {
		fn active_key_id(&self) -> u32 {
			if self.asked.fetch_add(1, Ordering::SeqCst) == 0 {
				1
			} else {
				2
			}
		}

		fn key_state(&self, key_id: u32) -> KeyState {
			if key_id == 2 {
				KeyState::Active
			} else {
				KeyState::DecryptOnly
			}
		}

		fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
			Ok(Zeroizing::new([key_id as u8; ROOT_KEY_LEN]))
		}
	}

	#[test]
	fn a_rotation_between_two_questions_to_the_manager_is_not_a_retired_key() {
		let km = RotatedMidLookup {
			asked: AtomicU32::new(0),
		};
		let key =
			FileKey::generate(&km, db_id(), CipherSuite::Aes256Gcm, FileKind::Wal, 6).unwrap();
		assert_eq!(key.identity().key_id, 2);
		assert!(key.can_seal());
	}

	/// A manager whose active key changes every time it is asked, which no manager does for long.
	struct AlwaysRotating {
		asked: AtomicU32,
	}

	impl KeyManager for AlwaysRotating {
		fn active_key_id(&self) -> u32 {
			self.asked.fetch_add(1, Ordering::SeqCst)
		}

		fn key_state(&self, _key_id: u32) -> KeyState {
			KeyState::DecryptOnly
		}

		fn root_key(&self, _key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
			Ok(Zeroizing::new([0x55; ROOT_KEY_LEN]))
		}
	}

	#[test]
	fn a_key_that_keeps_changing_under_the_lookup_is_a_retryable_failure() {
		let km = AlwaysRotating {
			asked: AtomicU32::new(0),
		};
		let err =
			FileKey::generate(&km, db_id(), CipherSuite::Aes256Gcm, FileKind::Wal, 6).unwrap_err();
		assert_eq!(err, EncryptionError::KeyProvider(KeyProviderFailure::Unavailable));
		assert!(err.is_retryable());
		// It gave up after a few questions instead of asking for ever.
		assert!(km.asked.load(Ordering::SeqCst) <= 2 * ACTIVE_KEY_ATTEMPTS as u32);
	}

	#[test]
	fn a_key_rebuilt_for_reading_equals_the_writers_key_but_cannot_seal() {
		let km = SoftwareKeyManager::new([0x11; ROOT_KEY_LEN]);
		let writer =
			FileKey::generate(&km, db_id(), CipherSuite::XChaCha20Poly1305, FileKind::Vlog, 12)
				.unwrap();
		let reader = FileKey::for_reading(&km, writer.identity(), *writer.salt()).unwrap();
		assert_eq!(reader.dek_for_test(), writer.dek_for_test());
		assert_eq!(reader.identity(), writer.identity());
		assert_eq!(reader.salt(), writer.salt());
		assert!(writer.can_seal());
		assert!(!reader.can_seal());
	}

	#[test]
	fn for_reading_refuses_what_it_cannot_read() {
		let km = SoftwareKeyManager::new([0x11; ROOT_KEY_LEN]);
		km.add_key(2, [0x22; ROOT_KEY_LEN]).unwrap();
		km.retire_key(2).unwrap();
		km.add_key(3, [0x33; ROOT_KEY_LEN]).unwrap();
		let identity =
			|key_id| FileIdentity::new(CipherSuite::Aes256Gcm, FileKind::Sst, key_id, 5, db_id());

		// A decrypt-only key reads fine.
		FileKey::for_reading(&km, identity(3), SALT).unwrap();
		assert_eq!(
			FileKey::for_reading(&km, identity(2), SALT).unwrap_err(),
			EncryptionError::KeyRetired
		);
		assert_eq!(
			FileKey::for_reading(&km, identity(9), SALT).unwrap_err(),
			EncryptionError::UnknownKeyId
		);
		assert_eq!(
			FileKey::for_reading(&km, identity(1), [0; SALT_LEN]).unwrap_err(),
			EncryptionError::MalformedHeader
		);
	}

	#[test]
	fn a_file_key_prints_no_key_material() {
		let key = FileKey::derive_for_test(&ROOT, SALT, identity_a(), true);
		let secret = key.dek_for_test();
		let header_secret = key.header_key_for_test().unwrap();
		for text in [format!("{key:?}"), format!("{key:#?}")] {
			assert_leaks_no_bytes(&text, &[&secret, &header_secret, &ROOT]);
			assert!(text.contains("FileKey"), "{text}");
		}
	}
}
