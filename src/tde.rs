//! Transparent Data Encryption (TDE) for SSTable and WAL blocks.
//!
//! Provides authenticated encryption at rest (AEAD) with key rotation support
//! and configurable cipher suites:
//! - AES-256-GCM (Hardware-accelerated standard AEAD)
//! - XChaCha20-Poly1305 (Constant-time software AEAD with 192-bit nonce)
//! - ChaCha20-BLAKE3 (High-throughput committing AEAD with keyed BLAKE3 MAC)

use std::collections::HashMap;
use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::Aes256Gcm;
use chacha20::cipher::{KeyIvInit, StreamCipher};
use chacha20poly1305::XChaCha20Poly1305;
use parking_lot::RwLock;
use rand::RngCore;
use subtle::ConstantTimeEq;

use crate::error::{Error, Result};

pub const KEY_ID_DEFAULT: u32 = 1;

/// Supported AEAD cipher suites for Transparent Data Encryption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum CipherSuite {
	/// AES-256-GCM authenticated encryption (12-byte nonce, 16-byte tag).
	/// Standard AEAD accelerated by hardware crypto instructions.
	#[default]
	Aes256Gcm = 1,
	/// XChaCha20-Poly1305 authenticated encryption (24-byte nonce, 16-byte tag).
	/// High-performance constant-time software cipher with 192-bit nonce.
	XChaCha20Poly1305 = 2,
	/// ChaCha20-BLAKE3 authenticated encryption (12-byte nonce, 32-byte BLAKE3 keyed MAC tag).
	/// Committing AEAD combining ChaCha20 stream cipher and BLAKE3 keyed MAC.
	ChaCha20Blake3 = 3,
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
			3 => Ok(Self::ChaCha20Blake3),
			other => Err(Error::Other(format!("Unknown cipher suite id: {other}"))),
		}
	}

	/// Returns the required nonce length in bytes for this cipher suite.
	pub const fn nonce_len(self) -> usize {
		match self {
			Self::Aes256Gcm => 12,
			Self::XChaCha20Poly1305 => 24,
			Self::ChaCha20Blake3 => 12,
		}
	}

	/// Returns the authentication tag length in bytes for this cipher suite.
	pub const fn tag_len(self) -> usize {
		match self {
			Self::Aes256Gcm => 16,
			Self::XChaCha20Poly1305 => 16,
			Self::ChaCha20Blake3 => 32,
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
#[derive(Debug, Default)]
pub struct SoftwareKeyManager {
	active_id: RwLock<u32>,
	keys: RwLock<HashMap<u32, Arc<[u8; 32]>>>,
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
	pub fn encrypt_with_nonce(&self, plaintext: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
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
			CipherSuite::ChaCha20Blake3 => encrypt_chacha20_blake3(&key, nonce, aad, plaintext)?,
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
			CipherSuite::ChaCha20Blake3 => decrypt_chacha20_blake3(&key, nonce, aad, ct_and_tag),
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

const BLAKE3_ENC_CONTEXT: &str = "surrealkv 2026-09-25 ChaCha20-BLAKE3 enc key";
const BLAKE3_MAC_CONTEXT: &str = "surrealkv 2026-09-25 ChaCha20-BLAKE3 mac key";

fn encrypt_chacha20_blake3(
	key: &[u8; 32],
	nonce: &[u8],
	aad: &[u8],
	plaintext: &[u8],
) -> Result<Vec<u8>> {
	let enc_key = blake3::derive_key(BLAKE3_ENC_CONTEXT, key);
	let mac_key = blake3::derive_key(BLAKE3_MAC_CONTEXT, key);

	let nonce_arr = chacha20::Nonce::try_from(nonce)
		.map_err(|e| Error::Other(format!("Invalid ChaCha20 nonce: {e}")))?;
	let mut ciphertext = plaintext.to_vec();
	let mut cipher = chacha20::ChaCha20::new((&enc_key).into(), &nonce_arr);
	cipher.apply_keystream(&mut ciphertext);

	let mut hasher = blake3::Hasher::new_keyed(&mac_key);
	hasher.update(&(aad.len() as u64).to_le_bytes());
	hasher.update(aad);
	hasher.update(&(ciphertext.len() as u64).to_le_bytes());
	hasher.update(&ciphertext);
	let tag = hasher.finalize();

	let mut output = ciphertext;
	output.extend_from_slice(tag.as_bytes());
	Ok(output)
}

fn decrypt_chacha20_blake3(
	key: &[u8; 32],
	nonce: &[u8],
	aad: &[u8],
	ciphertext_and_tag: &[u8],
) -> Result<Vec<u8>> {
	const TAG_LEN: usize = 32;
	if ciphertext_and_tag.len() < TAG_LEN {
		return Err(Error::Other("Ciphertext too short for BLAKE3 tag".to_string()));
	}

	let ct_len = ciphertext_and_tag.len() - TAG_LEN;
	let ciphertext = &ciphertext_and_tag[..ct_len];
	let tag = &ciphertext_and_tag[ct_len..];

	let mac_key = blake3::derive_key(BLAKE3_MAC_CONTEXT, key);
	let mut hasher = blake3::Hasher::new_keyed(&mac_key);
	hasher.update(&(aad.len() as u64).to_le_bytes());
	hasher.update(aad);
	hasher.update(&(ciphertext.len() as u64).to_le_bytes());
	hasher.update(ciphertext);
	let expected_tag = hasher.finalize();

	if expected_tag.as_bytes().ct_eq(tag).unwrap_u8() != 1 {
		return Err(Error::Other(
			"Block authentication tag mismatch (integrity failure)".to_string(),
		));
	}

	let enc_key = blake3::derive_key(BLAKE3_ENC_CONTEXT, key);
	let nonce_arr = chacha20::Nonce::try_from(nonce)
		.map_err(|e| Error::Other(format!("Invalid ChaCha20 nonce: {e}")))?;
	let mut plaintext = ciphertext.to_vec();
	let mut cipher = chacha20::ChaCha20::new((&enc_key).into(), &nonce_arr);
	cipher.apply_keystream(&mut plaintext);
	Ok(plaintext)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn test_suite_roundtrip_and_tamper(suite: CipherSuite) {
		let key = [0x42u8; 32];
		let km = Arc::new(SoftwareKeyManager::new(key));
		let cipher = BlockCipher::with_cipher_suite(km, suite);

		let plaintext = b"Hello, SurrealKV transparent data encryption with multiple ciphers!";

		let encrypted = cipher.encrypt(plaintext).unwrap();
		assert_ne!(encrypted, plaintext);

		// Verify self-describing cipher suite identifier in envelope
		assert_eq!(encrypted[4], suite.id());

		// Verify roundtrip decryption
		let decrypted = cipher.decrypt(&encrypted).unwrap();
		assert_eq!(decrypted, plaintext);

		// Tamper with payload byte
		let mut tampered = encrypted.clone();
		let last = tampered.len() - 1;
		tampered[last] ^= 0x01;
		assert!(cipher.decrypt(&tampered).is_err());

		// Tamper with header byte (e.g. cipher_suite or key_id)
		let mut tampered_header = encrypted;
		tampered_header[0] ^= 0x01; // tamper with key_id
		assert!(cipher.decrypt(&tampered_header).is_err());
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
	fn test_chacha20_blake3() {
		test_suite_roundtrip_and_tamper(CipherSuite::ChaCha20Blake3);
	}

	#[test]
	fn test_cross_cipher_decryption_with_same_key_manager() {
		let key = [0x77u8; 32];
		let km: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new(key));

		let c_aes = BlockCipher::with_cipher_suite(Arc::clone(&km), CipherSuite::Aes256Gcm);
		let c_xchacha =
			BlockCipher::with_cipher_suite(Arc::clone(&km), CipherSuite::XChaCha20Poly1305);
		let c_blake3 = BlockCipher::with_cipher_suite(km, CipherSuite::ChaCha20Blake3);

		let msg = b"Universal cross-cipher test message";

		let enc_aes = c_aes.encrypt(msg).unwrap();
		let enc_xchacha = c_xchacha.encrypt(msg).unwrap();
		let enc_blake3 = c_blake3.encrypt(msg).unwrap();

		// Any cipher instance should be able to decrypt any valid block because the envelope is
		// self-describing!
		assert_eq!(c_aes.decrypt(&enc_aes).unwrap(), msg);
		assert_eq!(c_aes.decrypt(&enc_xchacha).unwrap(), msg);
		assert_eq!(c_aes.decrypt(&enc_blake3).unwrap(), msg);

		assert_eq!(c_xchacha.decrypt(&enc_aes).unwrap(), msg);
		assert_eq!(c_xchacha.decrypt(&enc_xchacha).unwrap(), msg);
		assert_eq!(c_xchacha.decrypt(&enc_blake3).unwrap(), msg);

		assert_eq!(c_blake3.decrypt(&enc_aes).unwrap(), msg);
		assert_eq!(c_blake3.decrypt(&enc_xchacha).unwrap(), msg);
		assert_eq!(c_blake3.decrypt(&enc_blake3).unwrap(), msg);
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
	fn test_options_encryption_configuration() {
		use crate::Options;

		let opts = Options::default();
		assert!(opts.key_manager.is_none());
		assert_eq!(opts.encryption_cipher, CipherSuite::Aes256Gcm);

		let km: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new([0x33; 32]));
		let opts = Options::new().with_encryption(km, CipherSuite::ChaCha20Blake3);
		assert!(opts.key_manager.is_some());
		assert_eq!(opts.encryption_cipher, CipherSuite::ChaCha20Blake3);

		let cipher =
			BlockCipher::with_cipher_suite(opts.key_manager.unwrap(), opts.encryption_cipher);
		let ct = cipher.encrypt(b"test options").unwrap();
		assert_eq!(cipher.decrypt(&ct).unwrap(), b"test options");
	}
}
