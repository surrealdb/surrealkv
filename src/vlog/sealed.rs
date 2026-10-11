//! The layout of a sealed (encrypted) value-log file: its header, and how a file that a crash
//! left half made is told from one that is merely not readable with the key at hand.
//!
//! A plaintext file starts with the 31 byte [`VLogFileHeader`] and goes straight to its entries.
//! A sealed file starts the same way and carries the encryption header after it:
//!
//! ```text
//!   0  31  the file header, with `version` set to SEALED_VERSION
//!  31   4  CRC-32 (IEEE) of those 31 bytes, big-endian
//!  35  92  the encryption header, whose tag covers the 31 bytes
//! 127      the first entry
//! ```
//!
//! The file header has no checksum of its own in a plaintext file, and the encryption header's
//! tag cannot tell damage from a wrong key: a flipped bit in `created_at` would make the tag fail,
//! and the file would be reported as sealed under another key. The CRC after the file header is
//! what keeps the two apart. It is checked before the key is touched, so damage in those bytes is
//! media corruption, and a tag that fails with the CRC intact is a key (or a forgery).
//!
//! The version of a sealed file is the plaintext version with the top bit set. A reader that
//! does not know sealed files finds a version it does not support and stops, instead of reading
//! ciphertext as entries, and a later change of the plaintext version moves both together.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use super::{VLogFileHeader, VLOG_FORMAT_VERSION};
use crate::error::{EncryptionError, Error, Result};
use crate::tde::{CryptoHeader, EncryptionContext, FileCipher, FileKind, HEADER_LEN};

/// The `version` of the file header of a sealed file.
pub(super) const SEALED_VERSION: u16 = VLOG_FORMAT_VERSION | 0x8000;

/// Length of the file header that every value-log file starts with.
const FILE_HEADER_LEN: usize = VLogFileHeader::SIZE;

/// Length of the checksum that follows the file header of a sealed file.
const CHECKSUM_LEN: usize = 4;

/// Length of the header of a sealed file, where its first entry starts.
pub(super) const SEALED_HEADER_LEN: usize = FILE_HEADER_LEN + CHECKSUM_LEN + HEADER_LEN;

/// The first bytes of every value-log file, plaintext or sealed.
const MAGIC: [u8; 4] = VLogFileHeader::MAGIC.to_be_bytes();

/// How many other names are tried for a file that is set aside before giving up.
const ASIDE_ATTEMPTS: u32 = 1000;

/// The header of a new sealed file, and the cipher that seals its entries.
///
/// Every call draws a new salt, so a file that is made again after a failure, or under an id
/// that an earlier file had, never shares a key with it.
fn encode_header(
	ctx: &EncryptionContext,
	file_id: u32,
	max_file_size: u64,
	compression: u8,
) -> Result<([u8; SEALED_HEADER_LEN], FileCipher)> {
	let mut file_header = VLogFileHeader::new(file_id, max_file_size, compression);
	file_header.version = SEALED_VERSION;
	let fixed = file_header.encode();

	// The tag covers every byte of the file header, so none of its fields can change without
	// the header failing to open.
	let (crypto, cipher) = ctx.create_file(FileKind::Vlog, u64::from(file_id), &fixed)?;

	let mut out = [0u8; SEALED_HEADER_LEN];
	out[..FILE_HEADER_LEN].copy_from_slice(&fixed);
	out[FILE_HEADER_LEN..FILE_HEADER_LEN + CHECKSUM_LEN]
		.copy_from_slice(&crc32fast::hash(&fixed).to_be_bytes());
	out[FILE_HEADER_LEN + CHECKSUM_LEN..].copy_from_slice(&crypto.encode());
	Ok((out, cipher))
}

/// Creates the file of a new sealed log, with its header written and fsynced, and returns it
/// with the cipher that seals its entries.
///
/// The file is created exclusively: a file that is already there is an error and is left alone,
/// never appended to. Nothing may be sealed under a key that an older file of the same name
/// used, and nothing that was written after a header that is not durable is worth having, so
/// the header is on disk before the first entry is written. The directory is the caller's to
/// fsync.
///
/// If writing or syncing the header fails, the file that was just created is set aside under
/// another name, which keeps a later start from finding a log file without a header between
/// two that have one. It holds nothing and is never deleted.
pub(super) fn create(
	path: &Path,
	file_id: u32,
	max_file_size: u64,
	compression: u8,
	ctx: &EncryptionContext,
) -> Result<(File, FileCipher)> {
	let (header, cipher) = encode_header(ctx, file_id, max_file_size, compression)?;
	let mut file = OpenOptions::new().append(true).create_new(true).open(path)?;
	let header_is_on_disk = file.write_all(&header).and_then(|()| file.sync_all());
	if let Err(e) = header_is_on_disk {
		match set_aside(path) {
			Ok(to) => tracing::warn!(
				"The header of VLog file {file_id} could not be made durable, so the file was set \
				 aside as {to:?}: {e}"
			),
			Err(aside) => tracing::error!(
				"The header of VLog file {file_id} could not be made durable ({e}) and the file \
				 could not be set aside: {aside}"
			),
		}
		return Err(e.into());
	}
	#[cfg(test)]
	{
		super::SYNCED_VLOG_FILES.lock().push(path.to_path_buf());
		super::SYNCED_VLOG_LENGTHS.lock().push((path.to_path_buf(), SEALED_HEADER_LEN as u64));
	}
	Ok((file, cipher))
}

/// Checks the header at the start of a sealed file and opens the file for reading, which is the
/// cipher that opens its entries and cannot seal.
///
/// `head` is the start of the file, at least the whole header. In order: the file header (magic,
/// and the id in it against the name of the file), its checksum, the encryption header (its own
/// checksum, then the database, the file, the key and the tag). Everything before the last step
/// is a statement about the bytes and none of it needs a key, so damage is never reported as
/// a wrong key. Any failure of the last step is a key, a different database or file, or a forgery,
/// and is never a reason to touch the file.
pub(super) fn open_header(
	ctx: &EncryptionContext,
	file_id: u32,
	head: &[u8],
) -> Result<FileCipher> {
	let too_short = || Error::Corruption(format!("VLog file {file_id} ends inside its header"));
	let (fixed, rest) = head.split_first_chunk::<FILE_HEADER_LEN>().ok_or_else(too_short)?;
	let file_header = VLogFileHeader::decode(fixed)?;
	if file_header.version != SEALED_VERSION {
		return Err(Error::Corruption(format!(
			"VLog file {file_id} is not a sealed file: its version is {}",
			file_header.version
		)));
	}
	let (stored, rest) = rest.split_first_chunk::<CHECKSUM_LEN>().ok_or_else(too_short)?;
	if crc32fast::hash(fixed).to_be_bytes() != *stored {
		return Err(Error::Corruption(format!(
			"The header of VLog file {file_id} does not match its checksum"
		)));
	}
	file_header.validate(file_id)?;

	let crypto = CryptoHeader::decode(rest)?;
	Ok(ctx.open_file(&crypto, FileKind::Vlog, u64::from(file_id), fixed)?)
}

/// Whether the whole of a file that is no longer than a sealed header is what a crash during its
/// creation leaves: nothing, zeros, a part of a header, or a whole header that is damaged. Such a
/// file holds no entry, because the header is made durable before the first entry is written.
///
/// What it must not be is a file whose header is intact and whose key does not fit, a plaintext
/// file or a file of a later format: those are never torn, however short. A header counts as
/// intact when both of its checksums hold, and a wrong key leaves them as they are.
pub(super) fn is_torn(file: &[u8]) -> bool {
	if file.len() > SEALED_HEADER_LEN {
		return false;
	}
	if file.iter().all(|&byte| byte == 0) {
		return true;
	}
	// A part of the header is the start of what was written: the magic, then the sealed version.
	let magic = file.len().min(MAGIC.len());
	if file.get(..magic) != MAGIC.get(..magic) {
		return false;
	}
	if let Some(version) = file.get(MAGIC.len()..MAGIC.len() + 2) {
		if version != SEALED_VERSION.to_be_bytes() {
			return false;
		}
	}
	if file.len() < SEALED_HEADER_LEN {
		return true;
	}

	let Some((fixed, rest)) = file.split_first_chunk::<FILE_HEADER_LEN>() else {
		return true;
	};
	let Some((stored, crypto)) = rest.split_first_chunk::<CHECKSUM_LEN>() else {
		return true;
	};
	if crc32fast::hash(fixed).to_be_bytes() != *stored {
		return true;
	}
	// A header that decodes, or that a later format wrote on purpose, is not damaged.
	matches!(CryptoHeader::decode(crypto), Err(EncryptionError::MalformedHeader))
}

/// Renames a file that must not be used, and must not be lost, to a name that the log does not
/// read: `<name>.torn`, or `<name>.torn.1` and so on if that is taken. Returns the new path.
pub(super) fn set_aside(path: &Path) -> io::Result<PathBuf> {
	let mut base = path.as_os_str().to_owned();
	base.push(".torn");
	let mut target = PathBuf::from(&base);
	for attempt in 1..=ASIDE_ATTEMPTS {
		if !target.exists() {
			std::fs::rename(path, &target)?;
			return Ok(target);
		}
		let mut name = base.clone();
		name.push(format!(".{attempt}"));
		target = PathBuf::from(name);
	}
	Err(io::Error::other("no free name to set a file aside under"))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use std::sync::Arc;

	use tempfile::TempDir;

	use super::*;
	use crate::tde::{CipherSuite, DbId, SoftwareKeyManager};

	const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];
	const FILE_ID: u32 = 0x0102_0304;
	const MAX_FILE_SIZE: u64 = 0x0A0B_0C0D_0E0F_1011;

	fn db_id() -> DbId {
		DbId::from_bytes([0xD0; 16])
	}

	fn context_with_root(suite: CipherSuite, root: u8) -> EncryptionContext {
		EncryptionContext::new(Arc::new(SoftwareKeyManager::new([root; 32])), db_id(), suite)
	}

	fn context(suite: CipherSuite) -> EncryptionContext {
		context_with_root(suite, 0x11)
	}

	/// A new sealed file under `dir`, and its bytes.
	fn made(dir: &TempDir, ctx: &EncryptionContext) -> (PathBuf, Vec<u8>) {
		let path = dir.path().join("00000000000000000001.vlog");
		let (file, _cipher) = create(&path, FILE_ID, MAX_FILE_SIZE, 0, ctx).unwrap();
		drop(file);
		let bytes = std::fs::read(&path).unwrap();
		(path, bytes)
	}

	/// The same bytes with the checksum of the encryption header fixed, so that a change reaches
	/// the checks that come after the checksum.
	fn with_fixed_crypto_crc(mut bytes: Vec<u8>) -> Vec<u8> {
		let crc = crc32fast::hash(&bytes[35..123]);
		bytes[123..127].copy_from_slice(&crc.to_be_bytes());
		bytes
	}

	/// The same bytes with the checksum of the file header fixed.
	fn with_fixed_file_crc(mut bytes: Vec<u8>) -> Vec<u8> {
		let crc = crc32fast::hash(&bytes[..31]);
		bytes[31..35].copy_from_slice(&crc.to_be_bytes());
		bytes
	}

	fn open(ctx: &EncryptionContext, bytes: &[u8]) -> Result<FileCipher> {
		open_header(ctx, FILE_ID, bytes)
	}

	fn encryption_error(result: Result<FileCipher>) -> EncryptionError {
		match result {
			Err(Error::Encryption(e)) => e,
			Err(other) => panic!("expected an encryption error, got {other:?}"),
			Ok(_) => panic!("expected an encryption error, the header opened"),
		}
	}

	fn is_corruption(result: Result<FileCipher>) -> bool {
		matches!(result, Err(Error::Corruption(_)))
	}

	#[test]
	fn a_sealed_header_has_the_documented_layout() {
		for suite in SUITES {
			let dir = TempDir::new().unwrap();
			let (_, bytes) = made(&dir, &context(suite));
			assert_eq!(bytes.len(), 127, "31 + 4 + 92");
			assert_eq!(bytes.len(), SEALED_HEADER_LEN);

			// The file header: the plaintext layout, with the sealed version.
			assert_eq!(bytes[0..4], *b"VLOG");
			assert_eq!(bytes[4..6], [0x80, 0x01]);
			assert_eq!(bytes[6..10], [0x01, 0x02, 0x03, 0x04]);
			assert_eq!(bytes[18..26], [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11]);
			assert_eq!(bytes[26], 0, "compression");
			assert_eq!(bytes[27..31], [0, 0, 0, 0], "reserved");

			// Its checksum, then the encryption header.
			assert_eq!(bytes[31..35], crc32fast::hash(&bytes[..31]).to_be_bytes());
			assert_eq!(bytes[35..39], *b"SKTD");
			let crypto = CryptoHeader::decode(&bytes[35..]).unwrap();
			assert_eq!(crypto.identity.file_kind, FileKind::Vlog);
			assert_eq!(crypto.identity.file_id, u64::from(FILE_ID));
			assert_eq!(crypto.identity.db_id, db_id());
			assert_eq!(crypto.identity.suite, suite);
		}
	}

	#[test]
	fn a_created_file_opens_for_reading_and_only_the_writer_can_seal() {
		for suite in SUITES {
			let dir = TempDir::new().unwrap();
			let ctx = context(suite);
			let path = dir.path().join("00000000000000000001.vlog");
			let (_file, writer) = create(&path, FILE_ID, MAX_FILE_SIZE, 0, &ctx).unwrap();
			let bytes = std::fs::read(&path).unwrap();

			let reader = open(&ctx, &bytes).unwrap();
			assert!(writer.can_seal());
			assert!(!reader.can_seal());
			assert_eq!(reader.identity(), writer.identity());

			// Whatever follows the header is not part of it.
			let mut with_entries = bytes.clone();
			with_entries.extend_from_slice(&[0xEE; 300]);
			open(&ctx, &with_entries).unwrap();
		}
	}

	#[test]
	fn creating_a_file_that_exists_is_an_error_and_leaves_it_alone() {
		let dir = TempDir::new().unwrap();
		let ctx = context(CipherSuite::Aes256Gcm);
		let path = dir.path().join("00000000000000000001.vlog");
		std::fs::write(&path, b"an entry that must survive").unwrap();

		let err = match create(&path, FILE_ID, MAX_FILE_SIZE, 0, &ctx) {
			Err(Error::Io(e)) => e.kind(),
			Err(other) => panic!("expected an I/O error, got {other:?}"),
			Ok(_) => panic!("an existing file was reopened"),
		};
		assert_eq!(err, io::ErrorKind::AlreadyExists);
		assert_eq!(std::fs::read(&path).unwrap(), b"an entry that must survive");
		let names: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
		assert_eq!(names.len(), 1, "nothing was set aside or created");
	}

	#[test]
	fn every_new_file_has_its_own_salt_and_so_its_own_key() {
		let dir = TempDir::new().unwrap();
		let ctx = context(CipherSuite::XChaCha20Poly1305);
		let first = dir.path().join("00000000000000000001.vlog");
		let second = dir.path().join("00000000000000000002.vlog");
		for path in [&first, &second] {
			create(path, FILE_ID, MAX_FILE_SIZE, 0, &ctx).unwrap();
		}
		let (a, b) = (std::fs::read(&first).unwrap(), std::fs::read(&second).unwrap());
		// The salt is bytes 75..107 of the file, and the tag follows it.
		assert_ne!(a[75..107], b[75..107]);
		assert_ne!(a[107..123], b[107..123]);
	}

	#[test]
	fn the_header_is_fsynced_before_create_returns() {
		let dir = TempDir::new().unwrap();
		let ctx = context(CipherSuite::Aes256Gcm);
		let path = dir.path().join("00000000000000000007.vlog");
		create(&path, FILE_ID, MAX_FILE_SIZE, 0, &ctx).unwrap();
		let lengths = super::super::SYNCED_VLOG_LENGTHS.lock();
		assert!(
			lengths.contains(&(path, 127)),
			"the file was fsynced with its whole header and nothing else in it"
		);
	}

	#[test]
	fn damage_in_the_file_header_is_corruption_and_never_a_wrong_key() {
		for suite in SUITES {
			let dir = TempDir::new().unwrap();
			let ctx = context(suite);
			let (_, bytes) = made(&dir, &ctx);
			// created_at, max_file_size, compression and the checksum itself: the fields the tag
			// covers and the checksum that guards them.
			for at in [10, 17, 18, 25, 26, 31, 34] {
				let mut damaged = bytes.clone();
				damaged[at] ^= 0x01;
				assert!(is_corruption(open(&ctx, &damaged)), "byte {at}: damage, not a wrong key");

				// With the checksum made to fit, the tag is what notices, and it is a key error.
				if at < 31 {
					assert_eq!(
						encryption_error(open(&ctx, &with_fixed_file_crc(damaged))),
						EncryptionError::WrongKey,
						"byte {at}"
					);
				}
			}
		}
	}

	#[test]
	fn a_header_that_names_another_file_is_refused_before_the_key_is_used() {
		let dir = TempDir::new().unwrap();
		let ctx = context(CipherSuite::Aes256Gcm);
		let (_, bytes) = made(&dir, &ctx);
		// Opened as the file with the next id: the id in the header is not the name of the file.
		assert!(is_corruption(open_header(&ctx, FILE_ID + 1, &bytes)));

		// Rewritten to say it is that file, checksum included: the encryption header still names
		// the old id.
		let mut renamed = bytes;
		renamed[6..10].copy_from_slice(&(FILE_ID + 1).to_be_bytes());
		let renamed = with_fixed_file_crc(renamed);
		assert_eq!(
			encryption_error(open_header(&ctx, FILE_ID + 1, &renamed)),
			EncryptionError::FileIdMismatch
		);
	}

	#[test]
	fn damage_in_the_encryption_header_is_a_malformed_header_until_its_checksum_fits() {
		for suite in SUITES {
			let dir = TempDir::new().unwrap();
			let ctx = context(suite);
			let (_, bytes) = made(&dir, &ctx);
			// (offset, what a damaged header whose checksum was fixed must be)
			let cases = [
				// The salt and the tag: the key or the tag no longer fit.
				(75, EncryptionError::WrongKey),
				(106, EncryptionError::WrongKey),
				(107, EncryptionError::WrongKey),
				(122, EncryptionError::WrongKey),
				// The database id and the file id.
				(51, EncryptionError::DbIdMismatch),
				(66, EncryptionError::DbIdMismatch),
				(67, EncryptionError::FileIdMismatch),
				(74, EncryptionError::FileIdMismatch),
			];
			for (at, expected) in cases {
				let mut damaged = bytes.clone();
				damaged[at] ^= 0x01;
				assert_eq!(
					encryption_error(open(&ctx, &damaged)),
					EncryptionError::MalformedHeader,
					"byte {at}: the checksum is what stops it"
				);
				assert_eq!(
					encryption_error(open(&ctx, &with_fixed_crypto_crc(damaged))),
					expected,
					"byte {at}"
				);
			}
			// The magic and the stored checksum of the encryption header.
			for at in [35, 36, 123, 126] {
				let mut damaged = bytes.clone();
				damaged[at] ^= 0x01;
				assert_eq!(
					encryption_error(open(&ctx, &damaged)),
					EncryptionError::MalformedHeader,
					"byte {at}"
				);
			}
		}
	}

	#[test]
	fn a_header_of_another_kind_of_file_does_not_open_as_a_value_log() {
		let dir = TempDir::new().unwrap();
		let ctx = context(CipherSuite::Aes256Gcm);
		let (_, bytes) = made(&dir, &ctx);
		// The encryption header of an SSTable with the same id, under the same key, tag covering
		// the same file header.
		let (sst, _) = ctx.create_file(FileKind::Sst, u64::from(FILE_ID), &bytes[..31]).unwrap();
		let mut forged = bytes[..35].to_vec();
		forged.extend_from_slice(&sst.encode());
		assert_eq!(
			encryption_error(open(&ctx, &forged)),
			EncryptionError::FileIdMismatch,
			"a header that says it belongs to an SSTable is not this file's"
		);
	}

	#[test]
	fn a_wrong_key_another_database_and_a_missing_key_are_named() {
		let dir = TempDir::new().unwrap();
		let ctx = context(CipherSuite::Aes256Gcm);
		let (_, bytes) = made(&dir, &ctx);

		let other_root = context_with_root(CipherSuite::Aes256Gcm, 0x99);
		assert_eq!(encryption_error(open(&other_root, &bytes)), EncryptionError::WrongKey);

		let other_db = EncryptionContext::new(
			Arc::new(SoftwareKeyManager::new([0x11; 32])),
			DbId::from_bytes([0xD1; 16]),
			CipherSuite::Aes256Gcm,
		);
		assert_eq!(encryption_error(open(&other_db, &bytes)), EncryptionError::DbIdMismatch);

		// A manager that has retired the key the file names.
		let km = Arc::new(SoftwareKeyManager::new([0x11; 32]));
		km.add_key(2, [0x22; 32]).unwrap();
		km.set_active_key_id(2).unwrap();
		km.retire_key(1).unwrap();
		let retired = EncryptionContext::new(km, db_id(), CipherSuite::Aes256Gcm);
		assert_eq!(encryption_error(open(&retired, &bytes)), EncryptionError::KeyRetired);
	}

	#[test]
	fn short_input_is_an_error_and_never_a_panic() {
		let dir = TempDir::new().unwrap();
		let ctx = context(CipherSuite::Aes256Gcm);
		let (_, bytes) = made(&dir, &ctx);
		for len in 0..SEALED_HEADER_LEN {
			let result = open(&ctx, &bytes[..len]);
			assert!(result.is_err(), "{len} bytes are not a header");
		}
		// Inside the encryption header, the answer is the one for a truncated header.
		assert_eq!(encryption_error(open(&ctx, &bytes[..100])), EncryptionError::MalformedHeader);
		// A plaintext header is not a sealed one.
		let plain = VLogFileHeader::new(FILE_ID, MAX_FILE_SIZE, 0).encode();
		assert!(is_corruption(open(&ctx, &plain)));
	}

	#[test]
	fn a_file_that_a_crash_left_half_made_is_torn() {
		let dir = TempDir::new().unwrap();
		let ctx = context(CipherSuite::Aes256Gcm);
		let (_, bytes) = made(&dir, &ctx);

		// Nothing, a part of the header, and the header with zeros after a part of it.
		assert!(is_torn(&[]));
		assert!(is_torn(&bytes[..1]));
		assert!(is_torn(&bytes[..4]));
		assert!(is_torn(&bytes[..6]));
		assert!(is_torn(&bytes[..31]));
		assert!(is_torn(&bytes[..100]));
		assert!(is_torn(&bytes[..126]));
		let mut zero_tail = bytes.clone();
		zero_tail[60..].fill(0);
		assert!(is_torn(&zero_tail), "the size was updated and the data was not");
		// Zeros only, of any length up to a header.
		assert!(is_torn(&[0u8; 1]));
		assert!(is_torn(&[0u8; 127]));
		// The whole header with damage in a checksum: the file header's or the encryption header's.
		let mut damaged = bytes.clone();
		damaged[33] ^= 1;
		assert!(is_torn(&damaged));
		let mut damaged = bytes;
		damaged[100] ^= 1;
		assert!(is_torn(&damaged));
	}

	#[test]
	fn a_file_that_is_not_half_made_is_never_torn() {
		let dir = TempDir::new().unwrap();
		let ctx = context(CipherSuite::XChaCha20Poly1305);
		let (_, bytes) = made(&dir, &ctx);

		// An intact header, which is all a file has until its first entry, is not torn, and
		// neither is one that a wrong key cannot open: the checksums hold either way.
		assert!(!is_torn(&bytes));
		// Longer than a header: it may hold entries.
		let mut longer = bytes.clone();
		longer.push(0);
		assert!(!is_torn(&longer));
		longer[100] ^= 1;
		assert!(!is_torn(&longer), "damage in a file that may hold entries is never set aside");
		// A plaintext file: its header, and a header with an entry, are the other mode's file.
		let plain = VLogFileHeader::new(FILE_ID, MAX_FILE_SIZE, 0).encode();
		assert!(!is_torn(&plain));
		assert!(!is_torn(&plain[..6]));
		let mut with_entry = plain.to_vec();
		with_entry.extend_from_slice(&[0, 0, 0, 3, 0, 0, 0, 3, b'k', b'e', b'y', b'v', b'a', b'l']);
		assert!(!is_torn(&with_entry));
		// Not a value log at all.
		assert!(!is_torn(b"hello"));
		assert!(!is_torn(&[0x56, 0x4C, 0x4F, 0x48]));
		// An encryption header of a later format, whose checksum holds: that is not damage.
		let mut later = bytes;
		later[42] = 1; // a reserved byte that version 1 requires to be zero
		let later = with_fixed_crypto_crc(later);
		assert!(!is_torn(&later));
	}

	#[test]
	fn a_file_set_aside_is_renamed_and_never_overwrites_one_that_is_there() {
		let dir = TempDir::new().unwrap();
		let path = dir.path().join("00000000000000000003.vlog");
		std::fs::write(&path, b"first").unwrap();
		let first = set_aside(&path).unwrap();
		assert_eq!(first, dir.path().join("00000000000000000003.vlog.torn"));
		assert!(!path.exists());
		assert_eq!(std::fs::read(&first).unwrap(), b"first");

		std::fs::write(&path, b"second").unwrap();
		let second = set_aside(&path).unwrap();
		assert_eq!(second, dir.path().join("00000000000000000003.vlog.torn.1"));
		assert_eq!(std::fs::read(&first).unwrap(), b"first", "the first file is as it was");
		assert_eq!(std::fs::read(&second).unwrap(), b"second");

		// The log does not read a name that it set aside.
		let opts = crate::Options::default();
		for name in ["00000000000000000003.vlog.torn", "00000000000000000003.vlog.torn.1"] {
			assert_eq!(opts.extract_vlog_file_id(name), None, "{name}");
		}
	}
}
