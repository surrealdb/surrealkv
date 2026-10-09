//! Cipher primitives for Transparent Data Encryption (TDE). Encryption at rest is not
//! implemented yet.
//!
//! The primitives here (AEAD with key rotation through a key manager) are not wired into any
//! SSTable, WAL or value-log path, so nothing the database writes is encrypted. Opening a
//! database with a key manager configured is refused, so data is never written unencrypted
//! by accident. The envelope format and the cipher suites are not final.
//!
//! Cipher suites:
//! - AES-256-GCM (Hardware-accelerated standard AEAD)
//! - XChaCha20-Poly1305 (Constant-time software AEAD with 192-bit nonce)

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::Aes256Gcm;
use chacha20poly1305::XChaCha20Poly1305;
use parking_lot::RwLock;
use rand::RngCore;

use crate::error::{Error, Result};

pub const KEY_ID_DEFAULT: u32 = 1;

/// Supported AEAD cipher suites for Transparent Data Encryption.
///
/// The discriminant is the suite id stored in the envelope. Id 3 was assigned to a suite that
/// was removed before any release; it is reserved and must never be reused for another
/// construction, so a stray id 3 can only ever be rejected and never misread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
#[repr(u8)]
pub enum CipherSuite {
	/// AES-256-GCM authenticated encryption (12-byte nonce, 16-byte tag).
	/// Standard AEAD accelerated by hardware crypto instructions.
	#[default]
	Aes256Gcm = 1,
	/// XChaCha20-Poly1305 authenticated encryption (24-byte nonce, 16-byte tag).
	/// High-performance constant-time software cipher with 192-bit nonce.
	XChaCha20Poly1305 = 2,
}

impl CipherSuite {
	/// Returns the 1-byte identifier for the cipher suite.
	pub const fn id(self) -> u8 {
		self as u8
	}

	/// Parses a cipher suite from its 1-byte identifier.
	pub fn from_id(id: u8) -> Result<Self> {
		match id {
			1 => Ok(Self::Aes256Gcm),
			2 => Ok(Self::XChaCha20Poly1305),
			3 => Err(Error::Other(
				"Cipher suite id 3 is reserved (it belonged to a suite removed before release) and is not supported"
					.to_string(),
			)),
			other => Err(Error::Other(format!("Unknown cipher suite id: {other}"))),
		}
	}

	/// Returns the required nonce length in bytes for this cipher suite.
	pub const fn nonce_len(self) -> usize {
		match self {
			Self::Aes256Gcm => 12,
			Self::XChaCha20Poly1305 => 24,
		}
	}

	/// Returns the authentication tag length in bytes for this cipher suite.
	pub const fn tag_len(self) -> usize {
		match self {
			Self::Aes256Gcm => 16,
			Self::XChaCha20Poly1305 => 16,
		}
	}

	/// Returns the envelope header length (key_id + cipher_suite + nonce).
	pub const fn header_len(self) -> usize {
		4 + 1 + self.nonce_len()
	}
}

/// Provider for encryption keys supporting key rotation via key ID.
pub trait KeyManager: Send + Sync {
	/// Returns the active (current) key ID for new writes.
	fn active_key_id(&self) -> u32;

	/// Retrieves the key bytes for a given key ID.
	fn get_key(&self, key_id: u32) -> Result<Arc<[u8; 32]>>;
}

/// In-memory software key manager with key rotation support.
#[derive(Default)]
pub struct SoftwareKeyManager {
	active_id: RwLock<u32>,
	keys: RwLock<HashMap<u32, Arc<[u8; 32]>>>,
}

// Written by hand because a derived `Debug` prints the key bytes, which then end up in logs
// and panic messages.
impl fmt::Debug for SoftwareKeyManager {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut key_ids: Vec<u32> = self.keys.read().keys().copied().collect();
		key_ids.sort_unstable();
		f.debug_struct("SoftwareKeyManager")
			.field("active_key_id", &*self.active_id.read())
			.field("key_ids", &key_ids)
			.finish_non_exhaustive()
	}
}

impl SoftwareKeyManager {
	pub fn new(initial_key: [u8; 32]) -> Self {
		let mut map = HashMap::new();
		map.insert(KEY_ID_DEFAULT, Arc::new(initial_key));
		Self {
			active_id: RwLock::new(KEY_ID_DEFAULT),
			keys: RwLock::new(map),
		}
	}

	pub fn add_key(&self, key_id: u32, key: [u8; 32]) {
		self.keys.write().insert(key_id, Arc::new(key));
	}

	pub fn set_active_key_id(&self, key_id: u32) -> Result<()> {
		if !self.keys.read().contains_key(&key_id) {
			return Err(Error::Other(format!("Cannot set active key: key ID {key_id} not found")));
		}
		*self.active_id.write() = key_id;
		Ok(())
	}
}

impl KeyManager for SoftwareKeyManager {
	fn active_key_id(&self) -> u32 {
		*self.active_id.read()
	}

	fn get_key(&self, key_id: u32) -> Result<Arc<[u8; 32]>> {
		self.keys
			.read()
			.get(&key_id)
			.cloned()
			.ok_or_else(|| Error::Other(format!("Encryption key {key_id} not found")))
	}
}

/// Encrypts and decrypts blocks with transparent authenticated encryption.
///
/// Encrypted format:
/// `[key_id: 4B BE] [cipher_suite: 1B] [nonce: NB] [ciphertext: ... NB] [tag: ... TB]`
pub struct BlockCipher {
	key_manager: Arc<dyn KeyManager>,
	cipher_suite: CipherSuite,
}

impl BlockCipher {
	/// Creates a new `BlockCipher` with default cipher suite (`AES-256-GCM`).
	pub fn new(key_manager: Arc<dyn KeyManager>) -> Self {
		Self::with_cipher_suite(key_manager, CipherSuite::default())
	}

	/// Creates a new `BlockCipher` configured with a specific cipher suite.
	pub fn with_cipher_suite(key_manager: Arc<dyn KeyManager>, cipher_suite: CipherSuite) -> Self {
		Self {
			key_manager,
			cipher_suite,
		}
	}

	/// Returns the currently configured cipher suite for new encryptions.
	pub fn cipher_suite(&self) -> CipherSuite {
		self.cipher_suite
	}

	/// Returns the associated key manager.
	pub fn key_manager(&self) -> &Arc<dyn KeyManager> {
		&self.key_manager
	}

	/// Encrypts plaintext block with authenticated encryption using a cryptographically secure
	/// random nonce.
	pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
		let nonce_len = self.cipher_suite.nonce_len();
		let mut nonce = vec![0u8; nonce_len];
		rand::rng().fill_bytes(&mut nonce);
		self.encrypt_with_nonce(plaintext, &nonce)
	}

	/// Encrypts plaintext block with authenticated encryption using an explicit nonce.
	///
	/// Not public: reusing a nonce under one key breaks AEAD security, so callers outside this
	/// module must go through `encrypt`, which draws a fresh random one.
	pub(crate) fn encrypt_with_nonce(&self, plaintext: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
		if nonce.len() != self.cipher_suite.nonce_len() {
			return Err(Error::Other(format!(
				"Invalid nonce length: expected {}, got {}",
				self.cipher_suite.nonce_len(),
				nonce.len()
			)));
		}

		let key_id = self.key_manager.active_key_id();
		let key = self.key_manager.get_key(key_id)?;

		let header_len = self.cipher_suite.header_len();
		let mut output =
			Vec::with_capacity(header_len + plaintext.len() + self.cipher_suite.tag_len());
		output.extend_from_slice(&key_id.to_be_bytes());
		output.push(self.cipher_suite.id());
		output.extend_from_slice(nonce);

		let aad = &output[..header_len];
		let ct_and_tag = match self.cipher_suite {
			CipherSuite::Aes256Gcm => encrypt_aes256_gcm(&key, nonce, aad, plaintext)?,
			CipherSuite::XChaCha20Poly1305 => {
				encrypt_xchacha20_poly1305(&key, nonce, aad, plaintext)?
			}
		};
		output.extend_from_slice(&ct_and_tag);
		Ok(output)
	}

	/// Decrypts and verifies authenticated block. Self-describing based on envelope header.
	pub fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
		if ciphertext.len() < 5 {
			return Err(Error::Other(
				"Ciphertext too short for key_id and cipher_suite".to_string(),
			));
		}

		let key_id = u32::from_be_bytes(ciphertext[..4].try_into().unwrap());
		let cipher_suite = CipherSuite::from_id(ciphertext[4])?;
		let header_len = cipher_suite.header_len();

		if ciphertext.len() < header_len + cipher_suite.tag_len() {
			return Err(Error::Other(
				"Ciphertext too short for header and authentication tag".to_string(),
			));
		}

		let nonce = &ciphertext[5..header_len];
		let aad = &ciphertext[..header_len];
		let ct_and_tag = &ciphertext[header_len..];

		let key = self.key_manager.get_key(key_id)?;

		match cipher_suite {
			CipherSuite::Aes256Gcm => decrypt_aes256_gcm(&key, nonce, aad, ct_and_tag),
			CipherSuite::XChaCha20Poly1305 => {
				decrypt_xchacha20_poly1305(&key, nonce, aad, ct_and_tag)
			}
		}
	}
}

// =============================================================================
// CIPHER SUITE IMPLEMENTATIONS
// =============================================================================

fn encrypt_aes256_gcm(
	key: &[u8; 32],
	nonce: &[u8],
	aad: &[u8],
	plaintext: &[u8],
) -> Result<Vec<u8>> {
	let cipher = Aes256Gcm::new_from_slice(key)
		.map_err(|e| Error::Other(format!("Failed to initialize AES-256-GCM: {e}")))?;
	let nonce = aes_gcm::Nonce::try_from(nonce)
		.map_err(|e| Error::Other(format!("Invalid AES-256-GCM nonce: {e}")))?;
	cipher
		.encrypt(
			&nonce,
			Payload {
				msg: plaintext,
				aad,
			},
		)
		.map_err(|e| Error::Other(format!("AES-256-GCM encryption failure: {e}")))
}

fn decrypt_aes256_gcm(
	key: &[u8; 32],
	nonce: &[u8],
	aad: &[u8],
	ciphertext_and_tag: &[u8],
) -> Result<Vec<u8>> {
	let cipher = Aes256Gcm::new_from_slice(key)
		.map_err(|e| Error::Other(format!("Failed to initialize AES-256-GCM: {e}")))?;
	let nonce = aes_gcm::Nonce::try_from(nonce)
		.map_err(|e| Error::Other(format!("Invalid AES-256-GCM nonce: {e}")))?;
	cipher
		.decrypt(
			&nonce,
			Payload {
				msg: ciphertext_and_tag,
				aad,
			},
		)
		.map_err(|e| {
			Error::Other(format!("Block authentication tag mismatch (integrity failure): {e}"))
		})
}

fn encrypt_xchacha20_poly1305(
	key: &[u8; 32],
	nonce: &[u8],
	aad: &[u8],
	plaintext: &[u8],
) -> Result<Vec<u8>> {
	let cipher = XChaCha20Poly1305::new_from_slice(key)
		.map_err(|e| Error::Other(format!("Failed to initialize XChaCha20-Poly1305: {e}")))?;
	let nonce = chacha20poly1305::XNonce::try_from(nonce)
		.map_err(|e| Error::Other(format!("Invalid XChaCha20-Poly1305 nonce: {e}")))?;
	cipher
		.encrypt(
			&nonce,
			Payload {
				msg: plaintext,
				aad,
			},
		)
		.map_err(|e| Error::Other(format!("XChaCha20-Poly1305 encryption failure: {e}")))
}

fn decrypt_xchacha20_poly1305(
	key: &[u8; 32],
	nonce: &[u8],
	aad: &[u8],
	ciphertext_and_tag: &[u8],
) -> Result<Vec<u8>> {
	let cipher = XChaCha20Poly1305::new_from_slice(key)
		.map_err(|e| Error::Other(format!("Failed to initialize XChaCha20-Poly1305: {e}")))?;
	let nonce = chacha20poly1305::XNonce::try_from(nonce)
		.map_err(|e| Error::Other(format!("Invalid XChaCha20-Poly1305 nonce: {e}")))?;
	cipher
		.decrypt(
			&nonce,
			Payload {
				msg: ciphertext_and_tag,
				aad,
			},
		)
		.map_err(|e| {
			Error::Other(format!("Block authentication tag mismatch (integrity failure): {e}"))
		})
}

#[cfg(test)]
mod tests {
	use super::*;

	/// True for the AEAD tag failure itself, as opposed to a parse or key-lookup failure that
	/// stops decryption before any AEAD check.
	fn is_auth_failure(err: &Error) -> bool {
		matches!(err, Error::Other(msg) if msg.contains("authentication tag mismatch"))
	}

	fn test_suite_roundtrip_and_tamper(suite: CipherSuite) {
		let key = [0x42u8; 32];
		let km = Arc::new(SoftwareKeyManager::new(key));
		// Key id 2 holds the same bytes as key id 1, so a tampered key id still resolves to a
		// real key and only the header binding in the AAD can reject the block.
		km.add_key(2, key);
		let cipher = BlockCipher::with_cipher_suite(km, suite);

		let plaintext = b"Hello, SurrealKV transparent data encryption with multiple ciphers!";

		let encrypted = cipher.encrypt(plaintext).unwrap();
		assert_ne!(encrypted, plaintext);

		// Verify self-describing cipher suite identifier in envelope
		assert_eq!(encrypted[4], suite.id());

		// Verify roundtrip decryption
		let decrypted = cipher.decrypt(&encrypted).unwrap();
		assert_eq!(decrypted, plaintext);

		let header_len = suite.header_len();
		let other_suite = if suite == CipherSuite::Aes256Gcm {
			CipherSuite::XChaCha20Poly1305
		} else {
			CipherSuite::Aes256Gcm
		};

		// Every tamper below must reach the AEAD check and fail there.
		let assert_auth_failure = |name: &str, tamper: &dyn Fn(&mut Vec<u8>)| {
			let mut tampered = encrypted.clone();
			tamper(&mut tampered);
			let err = cipher.decrypt(&tampered).unwrap_err();
			assert!(
				is_auth_failure(&err),
				"{suite:?}: tampered {name} must fail authentication, got {err:?}"
			);
		};
		// Key id 1 -> 2: a different but valid id holding the same key bytes.
		assert_auth_failure("key id", &|b| b[3] ^= 0x03);
		// Another valid suite, so the envelope is re-parsed with that suite's layout.
		assert_auth_failure("suite", &|b| b[4] = other_suite.id());
		assert_auth_failure("first nonce byte", &|b| b[5] ^= 0x01);
		assert_auth_failure("last nonce byte", &|b| b[header_len - 1] ^= 0x01);
		assert_auth_failure("first ciphertext byte", &|b| b[header_len] ^= 0x01);
		assert_auth_failure("last tag byte", &|b| *b.last_mut().unwrap() ^= 0x01);
		assert_auth_failure("truncated tag", &|b| b.truncate(b.len() - 1));

		// Control: a key id that resolves to no key fails in the key lookup, before any AEAD
		// check. The original header tamper only ever exercised this path.
		let mut unknown_key = encrypted;
		unknown_key[0] ^= 0x01;
		let err = cipher.decrypt(&unknown_key).unwrap_err();
		assert!(!is_auth_failure(&err), "{suite:?}: an unknown key id must not reach the AEAD");
		assert!(matches!(&err, Error::Other(msg) if msg.contains("not found")), "got {err:?}");
	}

	#[test]
	fn test_aes256_gcm() {
		test_suite_roundtrip_and_tamper(CipherSuite::Aes256Gcm);
	}

	#[test]
	fn test_xchacha20_poly1305() {
		test_suite_roundtrip_and_tamper(CipherSuite::XChaCha20Poly1305);
	}

	#[test]
	fn test_cross_cipher_decryption_with_same_key_manager() {
		let key = [0x77u8; 32];
		let km: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new(key));

		let c_aes = BlockCipher::with_cipher_suite(Arc::clone(&km), CipherSuite::Aes256Gcm);
		let c_xchacha = BlockCipher::with_cipher_suite(km, CipherSuite::XChaCha20Poly1305);

		let msg = b"Universal cross-cipher test message";

		let enc_aes = c_aes.encrypt(msg).unwrap();
		let enc_xchacha = c_xchacha.encrypt(msg).unwrap();

		// Any cipher instance should be able to decrypt any valid block because the envelope is
		// self-describing!
		assert_eq!(c_aes.decrypt(&enc_aes).unwrap(), msg);
		assert_eq!(c_aes.decrypt(&enc_xchacha).unwrap(), msg);

		assert_eq!(c_xchacha.decrypt(&enc_aes).unwrap(), msg);
		assert_eq!(c_xchacha.decrypt(&enc_xchacha).unwrap(), msg);
	}

	#[test]
	fn test_key_rotation() {
		let key1 = [0x11u8; 32];
		let key2 = [0x22u8; 32];

		let km = Arc::new(SoftwareKeyManager::new(key1));
		km.add_key(2, key2);

		let km_dyn: Arc<dyn KeyManager> = Arc::<SoftwareKeyManager>::clone(&km);
		let cipher = BlockCipher::with_cipher_suite(km_dyn, CipherSuite::Aes256Gcm);

		// Write block with key 1
		let msg1 = b"Message encrypted with key 1";
		let enc1 = cipher.encrypt(msg1).unwrap();
		assert_eq!(u32::from_be_bytes(enc1[..4].try_into().unwrap()), 1);

		// Rotate active key to key 2
		km.set_active_key_id(2).unwrap();

		// Write block with key 2
		let msg2 = b"Message encrypted with key 2";
		let enc2 = cipher.encrypt(msg2).unwrap();
		assert_eq!(u32::from_be_bytes(enc2[..4].try_into().unwrap()), 2);

		// Both blocks must decrypt transparently
		assert_eq!(cipher.decrypt(&enc1).unwrap(), msg1);
		assert_eq!(cipher.decrypt(&enc2).unwrap(), msg2);
	}

	#[test]
	fn test_cipher_suite_ids() {
		for suite in [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305] {
			assert_eq!(CipherSuite::from_id(suite.id()).unwrap(), suite);

			let km = Arc::new(SoftwareKeyManager::new([0x42; 32]));
			let cipher = BlockCipher::with_cipher_suite(km, suite);
			let encrypted = cipher.encrypt(b"suite id roundtrip").unwrap();
			assert_eq!(CipherSuite::from_id(encrypted[4]).unwrap(), suite);
			assert_eq!(cipher.decrypt(&encrypted).unwrap(), b"suite id roundtrip");
		}
		assert_eq!(CipherSuite::Aes256Gcm.id(), 1);
		assert_eq!(CipherSuite::XChaCha20Poly1305.id(), 2);

		// Id 3 belonged to a suite removed before release. It is reserved and must never be
		// handed out again, so decoding it is an explicit error rather than "unknown".
		let err = CipherSuite::from_id(3).unwrap_err();
		assert!(err.to_string().contains("reserved"), "got {err}");

		for id in [0u8, 4, 5, 255] {
			let err = CipherSuite::from_id(id).unwrap_err();
			assert!(err.to_string().contains("Unknown cipher suite"), "id {id}: got {err}");
		}
	}

	#[test]
	fn test_decrypt_rejects_reserved_suite_id() {
		let km = Arc::new(SoftwareKeyManager::new([0x42; 32]));
		let cipher = BlockCipher::new(km);

		// A well-formed envelope for key id 1 whose suite byte is 3, long enough that no
		// length check can fire before the suite is looked at.
		let mut envelope = vec![0, 0, 0, 1, 3];
		envelope.extend_from_slice(&[0xAB; 64]);

		let err = cipher.decrypt(&envelope).unwrap_err();
		assert!(err.to_string().contains("reserved"), "got {err}");
	}

	#[test]
	fn test_software_key_manager_debug_does_not_print_key_bytes() {
		// Non-repeating keys, so every textual form of them is easy to spot.
		let key1: [u8; 32] = std::array::from_fn(|i| 0xC0 + i as u8);
		let key7: [u8; 32] = std::array::from_fn(|i| 0xE0 + i as u8);
		let km = SoftwareKeyManager::new(key1);
		km.add_key(7, key7);
		km.set_active_key_id(7).unwrap();

		let compact = format!("{km:?}");
		let pretty = format!("{km:#?}");

		let mut forbidden: Vec<String> = Vec::new();
		for key in [key1, key7] {
			forbidden.push(format!("{key:?}"));
			forbidden.push(format!("{key:x?}"));
			forbidden.push(format!("{key:X?}"));
			forbidden.push(format!("{key:#x?}"));
			forbidden.push(key.iter().map(|b| format!("{b:02x}")).collect());
			forbidden.push(key.iter().map(|b| format!("{b:02X}")).collect());
			forbidden.push(key.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(", "));
		}
		for text in [&compact, &pretty] {
			for needle in &forbidden {
				assert!(!text.contains(needle.as_str()), "Debug output leaks key bytes: {text}");
			}
			// Any single key byte, in decimal or hex, would also be a leak. The key ids and the
			// field names cannot collide with these tokens.
			for token in text.split(|c: char| !c.is_ascii_alphanumeric()) {
				for b in key1.iter().chain(key7.iter()) {
					assert_ne!(token, b.to_string(), "Debug output leaks a key byte: {text}");
					assert_ne!(token.to_lowercase(), format!("{b:02x}"), "leak: {text}");
				}
			}
		}

		// Key ids and the active id are what an operator needs to see.
		assert!(compact.contains("active_key_id: 7"), "got {compact}");
		assert!(compact.contains("key_ids: [1, 7]"), "got {compact}");
	}

	#[test]
	fn test_options_encryption_configuration() {
		use crate::Options;

		let opts = Options::default();
		assert!(opts.key_manager.is_none());
		assert_eq!(opts.encryption_cipher, CipherSuite::Aes256Gcm);

		let km: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new([0x33; 32]));
		let opts = Options::new().with_encryption(km, CipherSuite::XChaCha20Poly1305);
		assert!(opts.key_manager.is_some());
		assert_eq!(opts.encryption_cipher, CipherSuite::XChaCha20Poly1305);

		let cipher =
			BlockCipher::with_cipher_suite(opts.key_manager.unwrap(), opts.encryption_cipher);
		let ct = cipher.encrypt(b"test options").unwrap();
		assert_eq!(cipher.decrypt(&ct).unwrap(), b"test options");
	}
}
