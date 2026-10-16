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
//! An entry of a sealed file is a single AEAD message, with nothing about it in the clear:
//!
//! ```text
//! key length (4)  key  value  tag (16)      (the first three are encrypted)
//! ```
//!
//! It is sealed at the byte offset of the entry in the file, which is its nonce slot, so an entry
//! cannot be moved, and with the id of the file and the database in its associated data, so
//! neither can it leave its file. The length of the key is sealed with the entry so that a
//! pointer cannot move the boundary between the key and the value, and the length of the whole
//! entry is the length that its pointer gives, so no length is stored beside it. What an offline
//! reader still sees is where each entry ends, which is what the next entry's position says,
//! and so the size of every key and value taken together.
//!
//! The version of a sealed file is the plaintext version with the top bit set. A reader that
//! does not know sealed files finds a version it does not support and stops, instead of reading
//! ciphertext as entries, and a later change of the plaintext version moves both together.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{entry_lens, VLogFileHeader, ValuePointer, VLOG_FORMAT_VERSION};
use crate::error::{EncryptionError, Error, Result};
use crate::tde::{
	BlockContext,
	BlockKind,
	CryptoHeader,
	EncryptionContext,
	FileCipher,
	FileKind,
	HEADER_LEN,
	TAG_LEN,
};
use crate::vfs::Fs;
use crate::CompressionType;

/// The `version` of the file header of a sealed file.
pub(crate) const SEALED_VERSION: u16 = VLOG_FORMAT_VERSION | 0x8000;

/// Length of the file header that every value-log file starts with.
const FILE_HEADER_LEN: usize = VLogFileHeader::SIZE;

/// Length of the checksum that follows the file header of a sealed file.
const CHECKSUM_LEN: usize = 4;

/// Length of the header of a sealed file, where its first entry starts.
pub(crate) const SEALED_HEADER_LEN: usize = FILE_HEADER_LEN + CHECKSUM_LEN + HEADER_LEN;

/// The first bytes of every value-log file, plaintext or sealed.
const MAGIC: [u8; 4] = VLogFileHeader::MAGIC.to_be_bytes();

/// How many other names are tried for a file that is set aside before giving up.
const ASIDE_ATTEMPTS: u32 = 1000;

/// Length of the length of the key, which opens what an entry seals.
pub(crate) const KEY_LEN_PREFIX: usize = 4;

/// What a sealed entry adds to its key and value: the length of the key, and the tag. Both suites
/// have a tag of the same length.
pub(crate) const ENTRY_OVERHEAD: usize = KEY_LEN_PREFIX + TAG_LEN;

/// The compression byte that an entry is sealed with. Entries are stored as they are given, and
/// the byte is part of what the entry is bound to.
const ENTRY_COMPRESSION: u8 = CompressionType::None as u8;

/// The header of a new sealed file, and the cipher that seals its entries.
///
/// Every call draws a new salt, so a file that is made again after a failure, or under an id
/// that an earlier file had, never shares a key with it.
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
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
/// Made with `std::fs` and not through the file system seam: a sealed value log is only made on
/// the operating system's file system, and is refused on any other before this is called (see
/// `VLogWriter::create_sealed_in`). Moving it is part of the encryption of a database in a
/// browser, which has not been done.
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
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) fn create(
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
///
/// `prepare` asks the key manager to make the key that the header names available before it is
/// used, which may wait on a key provider. A log does that when it is opened, and never for a
/// read, which a manager must not make wait.
pub(crate) fn open_header(
	ctx: &EncryptionContext,
	file_id: u32,
	head: &[u8],
	prepare: bool,
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
	if prepare {
		ctx.prepare(&[crypto.identity.key_id])?;
	}
	Ok(ctx.open_file(&crypto, FileKind::Vlog, u64::from(file_id), fixed)?)
}

/// Whether `head`, the start of a file, is the start of a sealed file's header: its magic and its
/// version. A file that is shorter than that is not recognised.
pub(crate) fn is_sealed_start(head: &[u8]) -> bool {
	head.starts_with(&MAGIC)
		&& head.get(MAGIC.len()..MAGIC.len() + 2) == Some(&SEALED_VERSION.to_be_bytes()[..])
}

/// Whether `head`, the start of a file, is the start of a plaintext file's header: its magic and
/// its version. A file that is shorter than that is not recognised.
pub(crate) fn is_plain_start(head: &[u8]) -> bool {
	head.starts_with(&MAGIC)
		&& head.get(MAGIC.len()..MAGIC.len() + 2) == Some(&VLOG_FORMAT_VERSION.to_be_bytes()[..])
}

/// Whether the whole of a file that is no longer than a sealed header is what a crash during its
/// creation leaves: nothing, zeros, a part of a header, or a whole header that is damaged. Such a
/// file holds no entry, because the header is made durable before the first entry is written.
///
/// What it must not be is a file whose header is intact and whose key does not fit, a plaintext
/// file or a file of a later format: those are never torn, however short. A header counts as
/// intact when both of its checksums hold, and a wrong key leaves them as they are.
pub(crate) fn is_torn(file: &[u8]) -> bool {
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

/// What an entry seals before it is sealed: the length of the key, the key and the value, in a
/// buffer that has room for the tag as well, so that sealing does not move it.
///
/// A refused allocation is an error here, before any position is taken, and leaves the writer as
/// it was.
pub(crate) fn frame(key: &[u8], value: &[u8]) -> Result<Vec<u8>> {
	let (key_len, _) = entry_lens(key.len(), value.len())?;
	let size = key
		.len()
		.checked_add(value.len())
		.and_then(|content| content.checked_add(ENTRY_OVERHEAD))
		.ok_or_else(|| Error::InvalidArgument("a VLog entry is too large".to_string()))?;
	let mut entry = Vec::new();
	entry.try_reserve_exact(size).map_err(|e| Error::Io(Arc::new(io::Error::from(e))))?;
	entry.extend_from_slice(&key_len.to_be_bytes());
	entry.extend_from_slice(key);
	entry.extend_from_slice(value);
	Ok(entry)
}

/// Seals an entry that [`frame`] made, in place, for the position `offset` of its file: the
/// ciphertext followed by the tag is what is stored.
///
/// The cipher takes the position for good the moment it is asked, and refuses a position that is
/// not after the last one it took, so a caller that tries again after a failure is told no,
/// instead of sealing different bytes under a nonce that was used.
pub(crate) fn seal(cipher: &FileCipher, offset: u64, entry: &mut Vec<u8>) -> Result<()> {
	let at = BlockContext::new(BlockKind::VlogEntry, offset, ENTRY_COMPRESSION);
	Ok(cipher.seal_append(&at, entry)?)
}

/// Opens an entry that was read from the file for `pointer`, and returns its value.
///
/// Nothing in `stored` is looked at before it has authenticated, except to compute its CRC-32,
/// which is checked first because it is cheap: a mismatch means the bytes are not what was
/// written, a pointer to something else, or damage, and is told apart from a failure of the tag,
/// where the bytes are what the pointer says and the key or the file is not.
///
/// The pointer is not trusted either: its sizes must give the length of what was read, and the
/// length of the key, which was sealed with the entry, must be the one the pointer gives.
pub(crate) fn open_entry(
	cipher: &FileCipher,
	pointer: &ValuePointer,
	mut stored: Vec<u8>,
) -> Result<Vec<u8>> {
	let place = || format!("file_id={}, offset={}", pointer.file_id, pointer.offset);
	if u64::try_from(stored.len()).ok() != Some(pointer.total_entry_size()) {
		return Err(Error::Corruption(format!(
			"A sealed VLog entry ({}) is {} bytes and its pointer gives other sizes",
			place(),
			stored.len()
		)));
	}
	if crc32fast::hash(&stored) != pointer.checksum {
		return Err(Error::Corruption(format!(
			"CRC32 mismatch of a sealed VLog entry ({})",
			place()
		)));
	}

	let at = BlockContext::new(BlockKind::VlogEntry, pointer.offset, ENTRY_COMPRESSION);
	let sealed_len = cipher.open_suffix(&at, &mut stored)?.len();
	stored.truncate(sealed_len);

	let key_len = stored.first_chunk::<KEY_LEN_PREFIX>().map(|prefix| u32::from_be_bytes(*prefix));
	let value_start = match key_len {
		Some(key_len) if key_len == pointer.key_size => KEY_LEN_PREFIX + key_len as usize,
		_ => {
			return Err(Error::Corruption(format!(
				"The pointer to a sealed VLog entry ({}) gives another length of the key than \
				 the entry",
				place()
			)))
		}
	};
	if value_start > stored.len() {
		return Err(Error::Corruption(format!(
			"A sealed VLog entry ({}) is too short for its key",
			place()
		)));
	}
	stored.drain(..value_start);
	Ok(stored)
}

/// Renames a file that must not be used, and must not be lost, to a name that the log does not
/// read: `<name>.torn`, or `<name>.torn.1` and so on if that is taken. Returns the new path.
///
/// On the operating system's file system. [`create`], which calls it with the file it just made
/// still open, is the one caller that is not given a file system.
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) fn set_aside(path: &Path) -> io::Result<PathBuf> {
	set_aside_in(&*crate::vfs::default_fs(), path)
}

/// As [`set_aside`], in `fs`. The file must not be open: a file system that keeps one handle per
/// path (a browser's) does not rename a file that is open.
pub(crate) fn set_aside_in(fs: &dyn Fs, path: &Path) -> io::Result<PathBuf> {
	let mut base = path.as_os_str().to_owned();
	base.push(".torn");
	let mut target = PathBuf::from(&base);
	for attempt in 1..=ASIDE_ATTEMPTS {
		if !fs.exists(&target) {
			fs.rename(path, &target)?;
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
	use crate::tde::{CipherSuite, DbId, FileIdentity, FileKey, SoftwareKeyManager};

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
		open_header(ctx, FILE_ID, bytes, true)
	}

	fn encryption_error<T>(result: Result<T>) -> EncryptionError {
		match result {
			Err(Error::Encryption(e)) => e,
			Err(other) => panic!("expected an encryption error, got {other:?}"),
			Ok(_) => panic!("expected an encryption error, it succeeded"),
		}
	}

	fn is_corruption<T>(result: Result<T>) -> bool {
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
		assert!(is_corruption(open_header(&ctx, FILE_ID + 1, &bytes, false)));

		// Rewritten to say it is that file, checksum included: the encryption header still names
		// the old id.
		let mut renamed = bytes;
		renamed[6..10].copy_from_slice(&(FILE_ID + 1).to_be_bytes());
		let renamed = with_fixed_file_crc(renamed);
		assert_eq!(
			encryption_error(open_header(&ctx, FILE_ID + 1, &renamed, false)),
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
	// The vectors below come from an independent computation (OpenSSL's AES-256-GCM and
	// ChaCha20-Poly1305, with HChaCha20 for the extended nonce, and the standard library's HMAC
	// for HKDF-SHA256 and zlib's CRC-32): the file key and the header key from the root, the salt
	// and the identity, the associated data and the nonce from the position, and the CRC-32 of
	// the stored bytes. Inputs: root bytes 00..1f, salt bytes 20..3f, database id bytes a0..af,
	// key id 1, file id 0x0A0B0C0D, created_at 0x0123456789ABCDEF, max_file_size 0x01000000.
	// Two entries per suite: the key "canary-key-1" with the 32 bytes 40..5f, at offset 127, and
	// an empty key with an empty value after it, at 191.
	const HEADER_AES: &str = "564c4f4780010a0b0c0d0123456789abcdef0000000001000000000000000022\
		f45a8e534b5444010102000000000100000000a0a1a2a3a4a5a6a7a8a9aaabac\
		adaeaf000000000a0b0c0d202122232425262728292a2b2c2d2e2f3031323334\
		35363738393a3b3c3d3e3f01ec24a91b60f89a99ad684fe7b589893376d34a";
	const ENTRY_0_AES: &str = "331116114ab8c10f4a4d6f00171aee5c3bfd819c35752cc34732b0de4eeff467\
		9a159fa5133965b611d35e2a7904d1938c39a896981f740c68848824a9e0cb4d";
	const ENTRY_0_AES_CRC: u32 = 0xb0d74020;
	const ENTRY_1_AES: &str = "78dff77a60d1cdca93e8363f6f783b44a0b7df13";
	const ENTRY_1_AES_CRC: u32 = 0x10a5bd58;
	const HEADER_XCHACHA: &str = "564c4f4780010a0b0c0d0123456789abcdef0000000001000000000000000022\
		f45a8e534b5444010202000000000100000000a0a1a2a3a4a5a6a7a8a9aaabac\
		adaeaf000000000a0b0c0d202122232425262728292a2b2c2d2e2f3031323334\
		35363738393a3b3c3d3e3f2c53c1514d14a2c170bd259fb0aa8970bd84cbf5";
	const ENTRY_0_XCHACHA: &str =
		"81d69f2c996dadf7cf07e37f0bac24f03e12f2c261bf6208496c46abbd61636b\
		2680987fd3181dec2c79d82c40c4a58dfe06a15291cc2509fd749f7ab0f23f32";
	const ENTRY_0_XCHACHA_CRC: u32 = 0x0803d573;
	const ENTRY_1_XCHACHA: &str = "0e98dc1b67ba2cbdeb53cd56849c1f45786f3673";
	const ENTRY_1_XCHACHA_CRC: u32 = 0x5049ec30;

	const KAT_FILE_ID: u32 = 0x0A0B_0C0D;

	fn kat_root() -> [u8; 32] {
		std::array::from_fn(|i| i as u8)
	}

	fn kat_salt() -> [u8; 32] {
		std::array::from_fn(|i| 0x20 + i as u8)
	}

	fn kat_db() -> DbId {
		DbId::from_bytes(std::array::from_fn(|i| 0xA0 + i as u8))
	}

	/// The cipher of the file of the vectors, which seals if `can_seal` and otherwise only opens.
	fn kat_cipher(suite: CipherSuite, can_seal: bool) -> FileCipher {
		let identity =
			FileIdentity::new(suite, FileKind::Vlog, 1, u64::from(KAT_FILE_ID), kat_db());
		FileCipher::new(FileKey::derive_for_test(&kat_root(), kat_salt(), identity, can_seal))
			.unwrap()
	}

	fn unhex(hex: &str) -> Vec<u8> {
		(0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect()
	}

	fn hex(bytes: &[u8]) -> String {
		bytes.iter().map(|byte| format!("{byte:02x}")).collect()
	}

	#[test]
	fn the_header_and_the_entries_match_independently_computed_vectors() {
		let entries: [(&[u8], Vec<u8>); 2] =
			[(&b"canary-key-1"[..], (0x40u8..0x60).collect()), (&b""[..], Vec::new())];
		let cases = [
			(
				CipherSuite::Aes256Gcm,
				HEADER_AES,
				[(ENTRY_0_AES, ENTRY_0_AES_CRC), (ENTRY_1_AES, ENTRY_1_AES_CRC)],
			),
			(
				CipherSuite::XChaCha20Poly1305,
				HEADER_XCHACHA,
				[(ENTRY_0_XCHACHA, ENTRY_0_XCHACHA_CRC), (ENTRY_1_XCHACHA, ENTRY_1_XCHACHA_CRC)],
			),
		];
		for (suite, header, stored) in cases {
			// The header says what the computation put into it, and opens under the key.
			let header = unhex(header);
			assert_eq!(header.len(), SEALED_HEADER_LEN);
			let file_header = VLogFileHeader::decode(&header[..31]).unwrap();
			assert_eq!(file_header.version, 0x8001);
			assert_eq!(file_header.file_id, KAT_FILE_ID);
			assert_eq!(file_header.created_at, 0x0123_4567_89AB_CDEF);
			assert_eq!(file_header.max_file_size, 0x0100_0000);
			assert_eq!(file_header.compression, 0);
			let ctx = EncryptionContext::new(
				Arc::new(SoftwareKeyManager::new(kat_root())),
				kat_db(),
				suite,
			);
			open_header(&ctx, KAT_FILE_ID, &header, false).unwrap();

			// The entries are what sealing them gives, and open as the pointers say.
			let writer = kat_cipher(suite, true);
			let reader = kat_cipher(suite, false);
			let mut offset = SEALED_HEADER_LEN as u64;
			for ((key, value), (expected, crc)) in entries.iter().zip(stored) {
				let expected = unhex(expected);
				let mut entry = frame(key, value).unwrap();
				seal(&writer, offset, &mut entry).unwrap();
				assert_eq!(hex(&entry), hex(&expected), "{suite:?}, the entry at {offset}");
				assert_eq!(crc32fast::hash(&entry), crc);

				let pointer = ValuePointer::sealed(
					KAT_FILE_ID,
					offset,
					key.len() as u32,
					value.len() as u32,
					crc,
				);
				assert_eq!(pointer.total_entry_size(), expected.len() as u64);
				assert_eq!(open_entry(&reader, &pointer, expected.clone()).unwrap(), *value);
				offset += expected.len() as u64;
			}
		}
	}

	#[test]
	fn a_sealed_entry_is_as_long_as_its_pointer_says() {
		for suite in SUITES {
			let (writer, reader) = (kat_cipher(suite, true), kat_cipher(suite, false));
			let mut offset = 127u64;
			for (key_len, value_len) in
				[(0usize, 0usize), (1, 0), (0, 1), (5, 100), (12, 0), (300, 7)]
			{
				let (key, value) = (vec![b'k'; key_len], vec![b'v'; value_len]);
				let mut entry = frame(&key, &value).unwrap();
				assert_eq!(entry.len(), 4 + key_len + value_len, "the length of the key and both");
				assert!(entry.capacity() >= entry.len() + 16, "room for the tag");
				seal(&writer, offset, &mut entry).unwrap();
				assert_eq!(entry.len(), 4 + key_len + value_len + 16);

				let pointer = ValuePointer::sealed(
					1,
					offset,
					key_len as u32,
					value_len as u32,
					crc32fast::hash(&entry),
				);
				assert_eq!(pointer.total_entry_size(), entry.len() as u64);
				offset += entry.len() as u64;
				assert_eq!(open_entry(&reader, &pointer, entry).unwrap(), value);
			}
		}
		assert_eq!(frame(b"", b"").unwrap(), [0, 0, 0, 0]);
		assert_eq!(frame(b"ab", b"c").unwrap(), [0, 0, 0, 2, b'a', b'b', b'c']);
	}

	#[test]
	fn the_size_of_an_entry_depends_on_whether_its_pointer_says_it_is_sealed() {
		let plain = ValuePointer::new(1, 0, 3, 5, 0);
		let sealed = ValuePointer::sealed(1, 0, 3, 5, 0);
		assert!(!plain.is_sealed() && sealed.is_sealed());
		// A plaintext entry: both lengths, the key, the value and the CRC. A sealed one: the
		// length of the key, the key, the value and the tag.
		assert_eq!(plain.total_entry_size(), 8 + 3 + 5 + 4);
		assert_eq!(sealed.total_entry_size(), 4 + 3 + 5 + 16);
		for suite in SUITES {
			assert_eq!(suite.tag_len(), 16, "the pointer does not know the suite");
		}

		// The pointer is the same 25 bytes, and says which kind it is in its first byte.
		let encoded = sealed.encode();
		assert_eq!(encoded.len(), 25);
		assert_eq!(encoded[0], 2);
		assert_eq!(ValuePointer::decode(&encoded).unwrap(), sealed);
		assert_eq!(plain.encode()[0], 1);

		// Sizes that are all the wire allows cannot overflow.
		let largest = ValuePointer::sealed(1, 0, u32::MAX, u32::MAX, 0);
		assert_eq!(largest.total_entry_size(), 2 * u64::from(u32::MAX) + 20);
	}

	#[test]
	fn every_changed_byte_of_a_sealed_entry_is_caught_before_anything_is_decoded() {
		for suite in SUITES {
			let (writer, reader) = (kat_cipher(suite, true), kat_cipher(suite, false));
			let mut stored = frame(b"key", b"value-bytes").unwrap();
			seal(&writer, 200, &mut stored).unwrap();
			let pointer = ValuePointer::sealed(1, 200, 3, 11, crc32fast::hash(&stored));
			assert_eq!(open_entry(&reader, &pointer, stored.clone()).unwrap(), b"value-bytes");

			for at in 0..stored.len() {
				let mut bad = stored.clone();
				bad[at] ^= 0x80;
				// The CRC that the pointer holds is what stops it first.
				assert!(
					is_corruption(open_entry(&reader, &pointer, bad.clone())),
					"{suite:?}, byte {at}"
				);
				// With the CRC made to fit, as if the pointer had been changed along with it, it is
				// the tag.
				let fitted = ValuePointer {
					checksum: crc32fast::hash(&bad),
					..pointer.clone()
				};
				assert_eq!(
					encryption_error(open_entry(&reader, &fitted, bad)),
					EncryptionError::AuthenticationFailed,
					"{suite:?}, byte {at}"
				);
			}
		}
	}

	#[test]
	fn a_pointer_that_lies_about_where_or_what_the_entry_is_does_not_open_it() {
		for suite in SUITES {
			let (writer, reader) = (kat_cipher(suite, true), kat_cipher(suite, false));
			let mut stored = frame(b"key", b"value-bytes").unwrap();
			seal(&writer, 200, &mut stored).unwrap();
			let honest = ValuePointer::sealed(1, 200, 3, 11, crc32fast::hash(&stored));

			// Another position: the nonce and the associated data differ, and the bytes and their
			// CRC do not.
			for offset in [0u64, 1, 127, 199, 201, 216, 4096, u64::MAX] {
				let moved = ValuePointer {
					offset,
					..honest.clone()
				};
				assert_eq!(
					encryption_error(open_entry(&reader, &moved, stored.clone())),
					EncryptionError::AuthenticationFailed,
					"{suite:?}, offset {offset}"
				);
			}

			// Another file, with another salt, so another key.
			let identity =
				FileIdentity::new(suite, FileKind::Vlog, 1, u64::from(KAT_FILE_ID), kat_db());
			let elsewhere =
				FileCipher::new(FileKey::derive_for_test(&kat_root(), [0x77; 32], identity, false))
					.unwrap();
			assert_eq!(
				encryption_error(open_entry(&elsewhere, &honest, stored.clone())),
				EncryptionError::AuthenticationFailed
			);

			// The boundary between the key and the value moved, with the whole as long as it was:
			// the bytes are authentic and the length that was sealed with them says otherwise.
			for (key_size, value_size) in [(4, 10), (2, 12), (0, 14), (14, 0)] {
				let shifted = ValuePointer {
					key_size,
					value_size,
					..honest.clone()
				};
				assert_eq!(shifted.total_entry_size(), honest.total_entry_size());
				assert!(
					is_corruption(open_entry(&reader, &shifted, stored.clone())),
					"{suite:?}, key {key_size} value {value_size}"
				);
			}

			// Sizes that give another length than what was read, however large, and a pointer to a
			// plaintext entry: nothing is opened and nothing is allocated for them.
			for (key_size, value_size) in [(3, 12), (3, 10), (0, 0), (u32::MAX, u32::MAX)] {
				let longer = ValuePointer {
					key_size,
					value_size,
					..honest.clone()
				};
				assert!(is_corruption(open_entry(&reader, &longer, stored.clone())));
			}
			let plain = ValuePointer::new(1, 200, 3, 11, honest.checksum);
			assert!(is_corruption(open_entry(&reader, &plain, stored.clone())));
			assert!(is_corruption(open_entry(&reader, &honest, Vec::new())));
			assert!(is_corruption(open_entry(
				&reader,
				&honest,
				stored[..stored.len() - 1].to_vec()
			)));
		}
	}

	#[test]
	fn a_position_is_sealed_once() {
		for suite in SUITES {
			let writer = kat_cipher(suite, true);
			let mut first = frame(b"a", b"1").unwrap();
			seal(&writer, 127, &mut first).unwrap();

			// The same position and the ones before it, which is what a retry at the offset that a
			// failed write left behind would ask for, are refused and leave the entry as it was.
			for offset in [127u64, 126, 0] {
				let mut other = frame(b"b", b"2").unwrap();
				let before = other.clone();
				assert_eq!(
					encryption_error(seal(&writer, offset, &mut other)),
					EncryptionError::PositionReuse,
					"{suite:?}, offset {offset}"
				);
				assert_eq!(other, before);
			}
			let mut later = frame(b"c", b"3").unwrap();
			seal(&writer, 128, &mut later).unwrap();

			// A cipher made to open cannot seal, and neither can the cipher of another kind of
			// file.
			let mut entry = frame(b"d", b"4").unwrap();
			let reader = kat_cipher(suite, false);
			assert!(matches!(
				encryption_error(seal(&reader, 1000, &mut entry)),
				EncryptionError::InvalidInput(_)
			));
			let sst = FileCipher::new(FileKey::derive_for_test(
				&kat_root(),
				kat_salt(),
				FileIdentity::new(suite, FileKind::Sst, 1, 1, kat_db()),
				true,
			))
			.unwrap();
			assert!(matches!(
				encryption_error(seal(&sst, 1000, &mut entry)),
				EncryptionError::InvalidInput(_)
			));
		}
	}
}
