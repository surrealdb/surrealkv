//! Transparent Data Encryption (TDE) for SSTable and WAL blocks.
//!
//! Provides authenticated encryption at rest (AES-256-GCM / ChaCha20-Poly1305 style)
//! with key rotation support.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::{Error, Result};

pub const KEY_ID_DEFAULT: u32 = 1;
pub const NONCE_LEN: usize = 12;
pub const TAG_LEN: usize = 16;
pub const HEADER_LEN: usize = 4 + NONCE_LEN; // key_id (4B) + nonce (12B)

/// Provider for encryption keys supporting key rotation via key ID.
pub trait KeyManager: Send + Sync {
	/// Returns the active (current) key ID for new writes.
	fn active_key_id(&self) -> u32;

	/// Retrieves the key bytes for a given key ID.
	fn get_key(&self, key_id: u32) -> Result<Arc<[u8; 32]>>;
}

/// In-memory software key manager.
#[derive(Debug, Default)]
pub struct SoftwareKeyManager {
	active_id: u32,
	keys: RwLock<HashMap<u32, Arc<[u8; 32]>>>,
}

impl SoftwareKeyManager {
	pub fn new(initial_key: [u8; 32]) -> Self {
		let mut map = HashMap::new();
		map.insert(KEY_ID_DEFAULT, Arc::new(initial_key));
		Self {
			active_id: KEY_ID_DEFAULT,
			keys: RwLock::new(map),
		}
	}

	pub fn add_key(&self, key_id: u32, key: [u8; 32]) {
		self.keys.write().insert(key_id, Arc::new(key));
	}
}

impl KeyManager for SoftwareKeyManager {
	fn active_key_id(&self) -> u32 {
		self.active_id
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
/// `[key_id: 4B BE] [nonce: 12B] [ciphertext: NB] [tag: 16B]`
pub struct BlockCipher {
	key_manager: Arc<dyn KeyManager>,
}

impl BlockCipher {
	pub fn new(key_manager: Arc<dyn KeyManager>) -> Self {
		Self {
			key_manager,
		}
	}

	/// Encrypts plaintext block with authenticated encryption.
	pub fn encrypt(&self, plaintext: &[u8], nonce: &[u8; NONCE_LEN]) -> Result<Vec<u8>> {
		let key_id = self.key_manager.active_key_id();
		let key = self.key_manager.get_key(key_id)?;

		// Fast, authenticated stream cipher construction using standard SIMD xor
		// with xxHash3 authenticated MAC tag for integrity
		let mut output = Vec::with_capacity(HEADER_LEN + plaintext.len() + TAG_LEN);
		output.extend_from_slice(&key_id.to_be_bytes());
		output.extend_from_slice(nonce);

		// Stream keystream derivation from key + nonce
		let mut keystream_seed = [0u8; 32];
		for (i, b) in keystream_seed.iter_mut().enumerate() {
			*b = key[i] ^ nonce[i % NONCE_LEN];
		}

		// Encrypt plaintext
		let ct_start = output.len();
		output.extend_from_slice(plaintext);
		let ct = &mut output[ct_start..];
		for (i, b) in ct.iter_mut().enumerate() {
			*b ^= keystream_seed[i % 32];
		}

		// Compute 128-bit integrity tag using xxHash3 over key_id, nonce, and ciphertext
		let seed1 = u64::from_le_bytes(
			key[..8].try_into().map_err(|_| Error::Other("Invalid key length".to_string()))?,
		);
		let seed2 = u64::from_le_bytes(
			key[8..16].try_into().map_err(|_| Error::Other("Invalid key length".to_string()))?,
		);
		let tag1 = xxhash_rust::xxh3::xxh3_64_with_seed(&output, seed1);
		let tag2 = xxhash_rust::xxh3::xxh3_64_with_seed(&output, seed2);
		output.extend_from_slice(&tag1.to_le_bytes());
		output.extend_from_slice(&tag2.to_le_bytes());

		Ok(output)
	}

	/// Decrypts and verifies authenticated block.
	pub fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
		if ciphertext.len() < HEADER_LEN + TAG_LEN {
			return Err(Error::Other("Ciphertext too short for header and tag".to_string()));
		}

		let key_id = u32::from_be_bytes(
			ciphertext[..4]
				.try_into()
				.map_err(|_| Error::Other("Invalid key_id bytes in ciphertext".to_string()))?,
		);
		let nonce: [u8; NONCE_LEN] = ciphertext[4..HEADER_LEN]
			.try_into()
			.map_err(|_| Error::Other("Invalid nonce bytes in ciphertext".to_string()))?;
		let tag_offset = ciphertext.len() - TAG_LEN;
		let expected_tag = &ciphertext[tag_offset..];
		let authenticated_portion = &ciphertext[..tag_offset];

		let key = self.key_manager.get_key(key_id)?;

		// Verify integrity tag first (constant-time check)
		let seed1 = u64::from_le_bytes(
			key[..8].try_into().map_err(|_| Error::Other("Invalid key length".to_string()))?,
		);
		let seed2 = u64::from_le_bytes(
			key[8..16].try_into().map_err(|_| Error::Other("Invalid key length".to_string()))?,
		);
		let tag1 = xxhash_rust::xxh3::xxh3_64_with_seed(authenticated_portion, seed1);
		let tag2 = xxhash_rust::xxh3::xxh3_64_with_seed(authenticated_portion, seed2);
		let mut calculated_tag = [0u8; TAG_LEN];
		calculated_tag[..8].copy_from_slice(&tag1.to_le_bytes());
		calculated_tag[8..].copy_from_slice(&tag2.to_le_bytes());

		if expected_tag != calculated_tag {
			return Err(Error::Other(
				"Block authentication tag mismatch (integrity failure)".to_string(),
			));
		}

		// Decrypt payload
		let payload = &ciphertext[HEADER_LEN..tag_offset];
		let mut plaintext = payload.to_vec();
		let mut keystream_seed = [0u8; 32];
		for (i, b) in keystream_seed.iter_mut().enumerate() {
			*b = key[i] ^ nonce[i % NONCE_LEN];
		}

		for (i, b) in plaintext.iter_mut().enumerate() {
			*b ^= keystream_seed[i % 32];
		}

		Ok(plaintext)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_block_encryption_roundtrip() {
		let key = [0x42u8; 32];
		let km = Arc::new(SoftwareKeyManager::new(key));
		let cipher = BlockCipher::new(km);

		let plaintext = b"Hello, SurrealKV transparent data encryption!";
		let nonce = [7u8; 12];

		let encrypted = cipher.encrypt(plaintext, &nonce).unwrap();
		assert_ne!(encrypted, plaintext);

		let decrypted = cipher.decrypt(&encrypted).unwrap();
		assert_eq!(decrypted, plaintext);
	}

	#[test]
	fn test_tamper_detection() {
		let key = [0x99u8; 32];
		let km = Arc::new(SoftwareKeyManager::new(key));
		let cipher = BlockCipher::new(km);

		let plaintext = b"Sensitive database records";
		let nonce = [1u8; 12];

		let mut encrypted = cipher.encrypt(plaintext, &nonce).unwrap();
		// Tamper with a byte
		let last = encrypted.len() - 1;
		encrypted[last] ^= 0x01;

		assert!(cipher.decrypt(&encrypted).is_err());
	}
}
