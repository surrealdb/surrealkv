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
//! salt and the identity of the file into a key of its own for every file ([`FileKey`]), so two
//! files never share a key whatever a manager returns.
//!
//! # Blocks
//!
//! A [`FileCipher`] is built from a file key and seals or opens the blocks of that file in place
//! with a detached tag. Each block is bound, through its associated data, to the database, the
//! kind and id of the file, the key id, its position and what it holds ([`BlockContext`]), and
//! its nonce is derived from its position. A cipher refuses to seal at a position that is not
//! after the last one it sealed, and only a cipher built from a freshly generated key can seal at
//! all. The exact layouts are documented on [`BlockContext::aad`] and in the module that holds
//! them.

mod cipher;
mod context;
mod header;
mod identity;
mod kdf;
mod keys;
mod manifest;
mod random;
mod suite;
#[cfg(test)]
mod testutil;

pub use cipher::{BlockContext, BlockKind, FileCipher, AAD_LEN, TAG_LEN};
pub use context::EncryptionContext;
pub use header::{CryptoHeader, HEADER_LEN, HEADER_MAGIC, HEADER_VERSION};
pub use identity::{DbId, FileIdentity, FileKind, DB_ID_LEN};
pub use kdf::{FileKey, FILE_KEY_LEN};
pub use keys::{KeyManager, KeyState, SoftwareKeyManager, ROOT_KEY_LEN};
pub use manifest::{
	describe_manifest,
	describe_manifest_file,
	is_cut_inside_manifest_magic,
	is_sealed_manifest,
	open_manifest,
	seal_manifest,
	ManifestDescription,
	MANIFEST_CONTAINER_VERSION,
	MANIFEST_MAGIC,
	MANIFEST_PREFIX_LEN,
};
pub use random::{fresh_salt, SALT_LEN};
pub use suite::{hardware_aes_available, CipherPreference, CipherSuite};
/// The wrapper `KeyManager::root_key` returns roots in, re-exported so that an implementation
/// does not need a dependency on the exact version of `zeroize` this crate uses.
pub use zeroize::Zeroizing;

/// The key id a [`SoftwareKeyManager`] gives the key it is created with.
pub const KEY_ID_DEFAULT: u32 = 1;

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::Options;

	#[test]
	fn options_carry_the_key_manager_and_the_cipher_preference() {
		let opts = Options::default();
		assert!(opts.key_manager.is_none());
		assert_eq!(opts.encryption_cipher, CipherPreference::Auto);

		let km: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new([0x33; ROOT_KEY_LEN]));
		let opts = Options::new().with_encryption(Arc::clone(&km), CipherSuite::XChaCha20Poly1305);
		let configured = opts.key_manager.as_ref().expect("the key manager was configured");
		assert_eq!(configured.active_key_id(), KEY_ID_DEFAULT);
		assert_eq!(opts.encryption_cipher, CipherPreference::Suite(CipherSuite::XChaCha20Poly1305));

		// A preference is accepted as it is, so the choice can be left to the library.
		let opts = Options::new().with_encryption(km, CipherPreference::Auto);
		assert_eq!(opts.encryption_cipher, CipherPreference::Auto);
	}
}
