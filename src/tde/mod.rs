//! Building blocks for encryption at rest (transparent data encryption, TDE).
//!
//! Encryption at rest is not implemented yet. Nothing the database writes goes through this
//! module, and opening a database with a key manager configured is refused by
//! `Options::validate`, so data is never written unencrypted by accident. The types here are
//! the foundation the SSTable, value-log, WAL and manifest writers will be built on, and none
//! of their formats is frozen.
//!
//! # Keys
//!
//! A [`KeyManager`] hands out one 32 byte root key per key id and says which ids may still be
//! used. Roots are never used to encrypt data: the engine mixes each root with a fresh random
//! salt and the identity of the file into a key of its own for every file, so two files never
//! share a key whatever a manager returns.

mod keys;
mod suite;
#[cfg(test)]
mod testutil;

pub use keys::{KeyManager, KeyState, SoftwareKeyManager, ROOT_KEY_LEN};
pub use suite::CipherSuite;

/// The key id a [`SoftwareKeyManager`] gives the key it is created with.
pub const KEY_ID_DEFAULT: u32 = 1;

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::Options;

	#[test]
	fn options_carry_the_key_manager_and_the_cipher_suite() {
		let opts = Options::default();
		assert!(opts.key_manager.is_none());
		assert_eq!(opts.encryption_cipher, CipherSuite::Aes256Gcm);

		let km: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new([0x33; ROOT_KEY_LEN]));
		let opts = Options::new().with_encryption(km, CipherSuite::XChaCha20Poly1305);
		let km = opts.key_manager.as_ref().expect("the key manager was configured");
		assert_eq!(km.active_key_id(), KEY_ID_DEFAULT);
		assert_eq!(opts.encryption_cipher, CipherSuite::XChaCha20Poly1305);
	}
}
