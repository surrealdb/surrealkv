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

	/// The suite the library picks when the caller leaves the choice open, given whether the CPU
	/// has hardware AES: AES-256-GCM if it does, XChaCha20-Poly1305 if not, since AES without
	/// hardware support is both slow and harder to make constant-time.
	pub const fn for_hardware_aes(hardware_aes: bool) -> Self {
		if hardware_aes {
			Self::Aes256Gcm
		} else {
			Self::XChaCha20Poly1305
		}
	}

	/// The suite for a new database when the caller leaves the choice open, resolved for the CPU
	/// this runs on.
	///
	/// Call it once, when the database is created, and persist the result: the answer depends on
	/// the machine, so it must never be asked again for an existing database, whose files stay in
	/// the suite they were written with. On WebAssembly it is always XChaCha20-Poly1305.
	pub fn resolve_auto() -> Self {
		Self::for_hardware_aes(hardware_aes_available())
	}
}

/// Whether this CPU has the instructions AES-256-GCM needs to be fast and constant-time: AES
/// rounds and carry-less multiplication for GHASH. Detected at run time on x86, x86-64 and
/// aarch64, always true on Apple's aarch64 and always false elsewhere, which includes
/// WebAssembly.
pub fn hardware_aes_available() -> bool {
	detect_hardware_aes()
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn detect_hardware_aes() -> bool {
	std::arch::is_x86_feature_detected!("aes") && std::arch::is_x86_feature_detected!("pclmulqdq")
}

// Every aarch64 CPU Apple has made implements the AES and polynomial multiply instructions, so
// there is nothing to detect and no reliance on the standard library's detection support on each
// of Apple's operating systems.
#[cfg(all(target_arch = "aarch64", target_vendor = "apple"))]
fn detect_hardware_aes() -> bool {
	true
}

#[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
fn detect_hardware_aes() -> bool {
	std::arch::is_aarch64_feature_detected!("aes")
		&& std::arch::is_aarch64_feature_detected!("pmull")
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
fn detect_hardware_aes() -> bool {
	false
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
	fn auto_picks_aes_only_with_hardware_support() {
		assert_eq!(CipherSuite::for_hardware_aes(true), CipherSuite::Aes256Gcm);
		assert_eq!(CipherSuite::for_hardware_aes(false), CipherSuite::XChaCha20Poly1305);
		// The resolver is the same rule applied to this machine, and answers the same every time.
		let resolved = CipherSuite::resolve_auto();
		assert_eq!(resolved, CipherSuite::for_hardware_aes(hardware_aes_available()));
		assert_eq!(resolved, CipherSuite::resolve_auto());
	}

	#[test]
	#[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
	fn targets_without_aes_instructions_resolve_to_xchacha() {
		assert!(!hardware_aes_available());
		assert_eq!(CipherSuite::resolve_auto(), CipherSuite::XChaCha20Poly1305);
	}

	#[test]
	fn nonce_and_tag_lengths_match_the_aead_constructions() {
		assert_eq!(CipherSuite::Aes256Gcm.nonce_len(), 12);
		assert_eq!(CipherSuite::XChaCha20Poly1305.nonce_len(), 24);
		assert_eq!(CipherSuite::Aes256Gcm.tag_len(), 16);
		assert_eq!(CipherSuite::XChaCha20Poly1305.tag_len(), 16);
	}
}
