//! Random bytes from the operating system, for salts and database ids.
//!
//! Always `getrandom`, never a userspace generator that could be cloned with a process: a
//! repeated salt under one root key repeats a file key, and with it every nonce.

use crate::error::{EncryptionError, KeyProviderFailure};

/// Length in bytes of the per-file salt.
pub const SALT_LEN: usize = 32;

/// Fills `buf` from the OS random number generator.
///
/// Fails with `KeyProvider(RandomSource)` if the generator fails, and also if it hands back
/// nothing but zeros, which a working generator does not do for a buffer of any useful size and
/// a broken one might. `buf` must not be empty.
pub(super) fn fill_nonzero(buf: &mut [u8]) -> Result<(), EncryptionError> {
	fill_nonzero_with(buf, |bytes| getrandom_04::fill(bytes).map_err(|_| ()))
}

fn fill_nonzero_with(
	buf: &mut [u8],
	fill: impl FnOnce(&mut [u8]) -> Result<(), ()>,
) -> Result<(), EncryptionError> {
	let random_source = EncryptionError::KeyProvider(KeyProviderFailure::RandomSource);
	if fill(buf).is_err() || buf.iter().all(|&byte| byte == 0) {
		return Err(random_source);
	}
	Ok(())
}

/// A fresh, non-zero salt for one file. Every writer draws its own: a retry, a repair, a file
/// that reuses an id after a crash all get a new one, so no two writers share a file key.
pub fn fresh_salt() -> Result<[u8; SALT_LEN], EncryptionError> {
	let mut salt = [0u8; SALT_LEN];
	fill_nonzero(&mut salt)?;
	Ok(salt)
}

#[cfg(test)]
mod tests {
	use super::*;

	const RANDOM_SOURCE: EncryptionError =
		EncryptionError::KeyProvider(KeyProviderFailure::RandomSource);

	#[test]
	fn a_failing_generator_is_a_key_provider_error_and_not_a_panic() {
		let mut buf = [0xAA; SALT_LEN];
		let err = fill_nonzero_with(&mut buf, |_| Err(())).unwrap_err();
		assert_eq!(err, RANDOM_SOURCE);
		assert!(!err.is_retryable());
	}

	#[test]
	fn an_all_zero_draw_is_rejected() {
		let mut buf = [0xAA; SALT_LEN];
		let err = fill_nonzero_with(&mut buf, |bytes| {
			bytes.fill(0);
			Ok(())
		})
		.unwrap_err();
		assert_eq!(err, RANDOM_SOURCE);
	}

	#[test]
	fn a_draw_with_a_single_non_zero_byte_is_accepted() {
		let mut buf = [0u8; SALT_LEN];
		fill_nonzero_with(&mut buf, |bytes| {
			bytes.fill(0);
			bytes[SALT_LEN - 1] = 1;
			Ok(())
		})
		.unwrap();
		assert_eq!(buf[SALT_LEN - 1], 1);
	}

	#[test]
	fn fresh_salts_are_non_zero_and_do_not_repeat() {
		let first = fresh_salt().unwrap();
		let second = fresh_salt().unwrap();
		assert_ne!(first, [0u8; SALT_LEN]);
		assert_ne!(second, [0u8; SALT_LEN]);
		// 256 random bits colliding is not something a test needs to allow for.
		assert_ne!(first, second);
	}
}
