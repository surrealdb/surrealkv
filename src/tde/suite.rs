//! The AEAD cipher suites and the ids they are stored under.

use crate::error::EncryptionError;

/// Supported AEAD cipher suites for Transparent Data Encryption.
///
/// The discriminant is the suite id stored in file headers. Id 3 was assigned to a suite that
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

	/// Parses a cipher suite from its 1-byte identifier. The reserved id 3 and every id that was
	/// never assigned are `UnsupportedSuite`.
	pub const fn from_id(id: u8) -> Result<Self, EncryptionError> {
		match id {
			1 => Ok(Self::Aes256Gcm),
			2 => Ok(Self::XChaCha20Poly1305),
			_ => Err(EncryptionError::UnsupportedSuite),
		}
	}

	/// Returns the nonce length in bytes of this suite's AEAD.
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
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn suite_ids_are_frozen_and_round_trip() {
		assert_eq!(CipherSuite::Aes256Gcm.id(), 1);
		assert_eq!(CipherSuite::XChaCha20Poly1305.id(), 2);
		for suite in [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305] {
			assert_eq!(CipherSuite::from_id(suite.id()).unwrap(), suite);
		}
	}

	#[test]
	fn the_reserved_and_unassigned_ids_are_rejected() {
		// Id 3 belonged to a suite removed before release and must never be handed out again.
		for id in [0u8, 3, 4, 5, 127, 255] {
			assert_eq!(
				CipherSuite::from_id(id).unwrap_err(),
				EncryptionError::UnsupportedSuite,
				"id {id}"
			);
		}
	}

	#[test]
	fn nonce_and_tag_lengths_match_the_aead_constructions() {
		assert_eq!(CipherSuite::Aes256Gcm.nonce_len(), 12);
		assert_eq!(CipherSuite::XChaCha20Poly1305.nonce_len(), 24);
		assert_eq!(CipherSuite::Aes256Gcm.tag_len(), 16);
		assert_eq!(CipherSuite::XChaCha20Poly1305.tag_len(), 16);
	}
}
