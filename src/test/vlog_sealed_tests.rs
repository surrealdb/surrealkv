//! Sealed value-log files at the level of the log: what is read back, what is refused and with
//! which error, and that nothing is repaired, renamed or deleted because a key does not fit.
//!
//! The files are made by hand with the pieces of the sealed layout, so these tests exercise the
//! reader on its own. The data is kept small: AEAD runs at a few MB/s in the debug profile.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use super::tfs;
use crate::error::{EncryptionError, Error, KeyProviderFailure};
use crate::tde::{
	CipherSuite,
	DbId,
	EncryptionContext,
	KeyManager,
	KeyState,
	SoftwareKeyManager,
	Zeroizing,
};
use crate::vlog::sealed::{self, SEALED_HEADER_LEN};
use crate::vlog::{VLog, VLogWriter, ValueLocation, ValuePointer, SYNCED_VLOG_LENGTHS};
use crate::{Options, VLogChecksumLevel};

const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

fn db_id() -> DbId {
	DbId::from_bytes([0xD0; 16])
}

fn context_with_root(suite: CipherSuite, root: u8) -> Arc<EncryptionContext> {
	Arc::new(EncryptionContext::new(Arc::new(SoftwareKeyManager::new([root; 32])), db_id(), suite))
}

fn context(suite: CipherSuite) -> Arc<EncryptionContext> {
	context_with_root(suite, 0x11)
}

/// Options for a log in `dir`, sealed under `ctx` if there is one. Every call has a block cache
/// of its own, so a value that one log read is never found in the cache of another. Checksum
/// verification is off: what is checked for a sealed file does not depend on it.
fn options(dir: &Path, ctx: Option<&Arc<EncryptionContext>>) -> Arc<Options> {
	let mut opts = Options {
		path: dir.to_path_buf(),
		vlog_max_file_size: 1 << 20,
		vlog_checksum_verification: VLogChecksumLevel::Disabled,
		..Default::default()
	};
	opts.encryption_context = ctx.cloned();
	tfs::create_dir_all(&opts, opts.vlog_dir()).unwrap();
	Arc::new(opts)
}

fn value_of(seed: u8, len: usize) -> Vec<u8> {
	(0..len).map(|i| seed.wrapping_add((i % 251) as u8)).collect()
}

/// A sealed file made by hand: its header and the entries, each sealed at its own offset. Returns
/// the pointer of each entry.
fn make_file(
	opts: &Options,
	ctx: &EncryptionContext,
	file_id: u32,
	entries: &[(&[u8], &[u8])],
) -> Vec<ValuePointer> {
	let path = opts.vlog_file_path(u64::from(file_id));
	let (mut file, cipher) =
		sealed::create(&path, file_id, opts.vlog_max_file_size, 0, ctx).unwrap();
	let mut offset = SEALED_HEADER_LEN as u64;
	let mut pointers = Vec::new();
	for (key, value) in entries {
		let mut stored = sealed::frame(key, value).unwrap();
		sealed::seal(&cipher, offset, &mut stored).unwrap();
		file.write_all(&stored).unwrap();
		pointers.push(ValuePointer::sealed(
			file_id,
			offset,
			key.len() as u32,
			value.len() as u32,
			crc32fast::hash(&stored),
		));
		offset += stored.len() as u64;
	}
	file.sync_all().unwrap();
	pointers
}

/// Every file of a directory by name, with its bytes.
fn snapshot(dir: &Path) -> BTreeMap<String, Vec<u8>> {
	std::fs::read_dir(dir)
		.unwrap()
		.map(|entry| {
			let entry = entry.unwrap();
			(entry.file_name().to_string_lossy().into_owned(), std::fs::read(entry.path()).unwrap())
		})
		.collect()
}

fn encryption_error<T>(result: crate::Result<T>) -> EncryptionError {
	match result {
		Err(Error::Encryption(e)) => e,
		Err(other) => panic!("expected an encryption error, got {other:?}"),
		Ok(_) => panic!("expected an encryption error, it succeeded"),
	}
}

fn is_corruption<T>(result: crate::Result<T>) -> bool {
	matches!(result, Err(Error::Corruption(_)))
}

fn fix_file_crc(bytes: &mut [u8]) {
	let crc = crc32fast::hash(&bytes[..31]);
	bytes[31..35].copy_from_slice(&crc.to_be_bytes());
}

fn fix_crypto_crc(bytes: &mut [u8]) {
	let crc = crc32fast::hash(&bytes[35..123]);
	bytes[123..127].copy_from_slice(&crc.to_be_bytes());
}

/// The same pointer with the CRC of the bytes that are in the file now, as if whoever changed
/// the file had changed the pointer along with it.
fn with_crc_of(pointer: &ValuePointer, file: &Path) -> ValuePointer {
	let bytes = std::fs::read(file).unwrap();
	let start = pointer.offset as usize;
	let end = start + pointer.total_entry_size() as usize;
	ValuePointer {
		checksum: crc32fast::hash(&bytes[start..end]),
		..pointer.clone()
	}
}

#[test]
fn a_sealed_log_reads_back_what_was_sealed() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let opts = options(dir.path(), Some(&ctx));
		let big = value_of(2, 3000);
		let first: Vec<(&[u8], &[u8])> = vec![
			(b"key-1", b"a small value"),
			(b"", b""),
			(b"a key and no value", b""),
			(b"", b"a value and no key"),
			(b"key-big", &big),
		];
		let second: Vec<(&[u8], &[u8])> = vec![(b"in-the-next-file", b"v"), (&[0xAB; 200], b"w")];
		let mut expected = make_file(&opts, &ctx, 1, &first)
			.into_iter()
			.zip(first.iter().map(|(_, value)| value.to_vec()))
			.collect::<Vec<_>>();
		expected.extend(
			make_file(&opts, &ctx, 2, &second)
				.into_iter()
				.zip(second.iter().map(|(_, value)| value.to_vec())),
		);
		let before = snapshot(&opts.vlog_dir());

		let vlog = VLog::new(Arc::clone(&opts)).unwrap();
		for (pointer, value) in &expected {
			assert_eq!(&vlog.get(pointer).unwrap(), value, "{suite:?}, {pointer:?}");
		}
		// Twice: the second read is the cache's.
		for (pointer, value) in &expected {
			assert_eq!(&vlog.get(pointer).unwrap(), value, "{suite:?}, {pointer:?}");
		}

		// The files are read and never written: the log has no writer, the next file is one past
		// the newest, and nothing in the directory changed.
		assert_eq!(vlog.file_handles.read().len(), 2);
		assert_eq!(vlog.next_file_id.load(Ordering::SeqCst), 3);
		assert_eq!(vlog.active_writer_id.load(Ordering::SeqCst), 2);
		assert!(vlog.writer.read().is_none(), "a sealed file is not reopened for appending");
		assert_eq!(snapshot(&opts.vlog_dir()), before);
	}
}

#[test]
fn the_newest_sealed_file_is_fsynced_by_the_first_sync() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(dir.path(), Some(&ctx));
	make_file(&opts, &ctx, 1, &[(b"k", b"v")]);
	let path = opts.vlog_file_path(1);
	// Creating the file fsynced it with nothing in it but its header, which is not what is looked
	// for: the length that an fsync of the whole file records is longer.
	let length = std::fs::metadata(&path).unwrap().len();
	assert!(length > SEALED_HEADER_LEN as u64, "the file holds an entry");
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	assert_eq!(vlog.unsynced_file_ids(), vec![1], "a run may have left it unsynced");
	assert!(!SYNCED_VLOG_LENGTHS.lock().contains(&(path.clone(), length)), "not synced yet");

	vlog.sync().unwrap();
	assert!(
		SYNCED_VLOG_LENGTHS.lock().contains(&(path, length)),
		"the first sync fsynced the file that no writer holds"
	);
	assert!(vlog.unsynced_file_ids().is_empty());
}

#[test]
fn a_sealed_entry_is_authenticated_whatever_the_checksum_level_says() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		for level in [VLogChecksumLevel::Disabled, VLogChecksumLevel::Full] {
			let dir = TempDir::new().unwrap();
			let ctx = context(suite);
			let mut opts = (*options(dir.path(), Some(&ctx))).clone();
			opts.vlog_checksum_verification = level;
			let opts = Arc::new(opts);
			let pointers =
				make_file(&opts, &ctx, 1, &[(b"key-0", b"value-0"), (b"key-1", b"value-1")]);

			// One byte of the second entry's ciphertext.
			let path = opts.vlog_file_path(1);
			let mut bytes = std::fs::read(&path).unwrap();
			bytes[pointers[1].offset as usize + 6] ^= 0x01;
			std::fs::write(&path, &bytes).unwrap();

			let vlog = VLog::new(Arc::clone(&opts)).unwrap();
			assert_eq!(vlog.get(&pointers[0]).unwrap(), b"value-0", "an entry that is intact");
			assert!(
				is_corruption(vlog.get(&pointers[1])),
				"{suite:?}, {level:?}: the CRC of the pointer"
			);
			// With the CRC made to fit the tag is what refuses it, and nothing is returned.
			let fitted = with_crc_of(&pointers[1], &path);
			assert_eq!(
				encryption_error(vlog.get(&fitted)),
				EncryptionError::AuthenticationFailed,
				"{suite:?}, {level:?}"
			);
		}
	}
}

#[test]
fn a_failed_authentication_keeps_its_kind_through_resolve_value() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::XChaCha20Poly1305);
	let opts = options(dir.path(), Some(&ctx));
	let pointers = make_file(&opts, &ctx, 1, &[(b"key", b"value")]);
	let path = opts.vlog_file_path(1);
	let mut bytes = std::fs::read(&path).unwrap();
	bytes[pointers[0].offset as usize] ^= 0x40;
	std::fs::write(&path, &bytes).unwrap();

	let vlog = Arc::new(VLog::new(Arc::clone(&opts)).unwrap());
	let tampered = ValueLocation::with_pointer(with_crc_of(&pointers[0], &path));
	assert_eq!(
		encryption_error(tampered.resolve_value(Some(&vlog))),
		EncryptionError::AuthenticationFailed,
		"a caller must be able to tell a forgery from a value that is missing"
	);
}

/// A log over one sealed file with one entry, in which byte `at` of the file was flipped and then
/// the checksums in `fixes` were made to fit; the result of opening the log over it, and the
/// directory before.
fn open_flipped(
	suite: CipherSuite,
	at: usize,
	fixes: &[fn(&mut [u8])],
) -> (crate::Result<VLog>, TempDir, BTreeMap<String, Vec<u8>>) {
	let dir = TempDir::new().unwrap();
	let ctx = context(suite);
	let opts = options(dir.path(), Some(&ctx));
	make_file(&opts, &ctx, 1, &[(b"key", b"value")]);
	let path = opts.vlog_file_path(1);
	let mut bytes = std::fs::read(&path).unwrap();
	assert!(bytes.len() > SEALED_HEADER_LEN, "the file holds an entry, so it cannot be torn");
	bytes[at] ^= 0x01;
	for fix in fixes {
		fix(&mut bytes);
	}
	std::fs::write(&path, &bytes).unwrap();
	let before = snapshot(&opts.vlog_dir());
	(VLog::new(opts), dir, before)
}

#[test]
fn damage_and_forgery_in_the_header_are_told_apart_and_leave_every_file_as_it_is() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let vlog_dir = |dir: &TempDir| dir.path().join("vlog");

		// created_at, compression and the checksum of the file header: damage, and with the
		// checksum made to fit, a key that does not authenticate the header.
		for at in [10, 26, 31] {
			let (result, dir, before) = open_flipped(suite, at, &[]);
			assert!(is_corruption(result), "{suite:?}, byte {at}");
			assert_eq!(snapshot(&vlog_dir(&dir)), before);
		}
		for at in [10, 26] {
			let (result, dir, before) = open_flipped(suite, at, &[fix_file_crc]);
			assert_eq!(encryption_error(result), EncryptionError::WrongKey, "{suite:?}, byte {at}");
			assert_eq!(snapshot(&vlog_dir(&dir)), before);
		}

		// The encryption header: its magic, salt and tag fail its own checksum, and with the
		// checksum made to fit, the salt and the tag fail the key, and the database and the
		// file id are named.
		for at in [35, 75, 107, 51, 67] {
			let (result, dir, before) = open_flipped(suite, at, &[]);
			assert_eq!(
				encryption_error(result),
				EncryptionError::MalformedHeader,
				"{suite:?}, byte {at}"
			);
			assert_eq!(snapshot(&vlog_dir(&dir)), before);
		}
		for (at, expected) in [
			(75, EncryptionError::WrongKey),
			(107, EncryptionError::WrongKey),
			(51, EncryptionError::DbIdMismatch),
			(67, EncryptionError::FileIdMismatch),
		] {
			let (result, dir, before) = open_flipped(suite, at, &[fix_crypto_crc]);
			assert_eq!(encryption_error(result), expected, "{suite:?}, byte {at}");
			assert_eq!(snapshot(&vlog_dir(&dir)), before);
		}
	}
}

#[test]
fn a_key_that_does_not_fit_is_an_error_and_never_a_reason_to_touch_a_file() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let opts = options(dir.path(), Some(&ctx));
		// One file with an entry, and a newer one that holds only its header: the case in which
		// a reader that mistook a wrong key for a file cut off by a crash would set it aside.
		make_file(&opts, &ctx, 1, &[(b"key", b"value")]);
		make_file(&opts, &ctx, 2, &[]);
		assert_eq!(std::fs::metadata(opts.vlog_file_path(2)).unwrap().len(), 127);
		let before = snapshot(&opts.vlog_dir());

		// Another root under the same key id.
		let other_root = options(dir.path(), Some(&context_with_root(suite, 0x99)));
		assert_eq!(encryption_error(VLog::new(other_root)), EncryptionError::WrongKey);
		assert_eq!(snapshot(&opts.vlog_dir()), before, "{suite:?}");

		// Another database.
		let other_db = Arc::new(EncryptionContext::new(
			Arc::new(SoftwareKeyManager::new([0x11; 32])),
			DbId::from_bytes([0xD1; 16]),
			suite,
		));
		let other_db = options(dir.path(), Some(&other_db));
		assert_eq!(encryption_error(VLog::new(other_db)), EncryptionError::DbIdMismatch);
		assert_eq!(snapshot(&opts.vlog_dir()), before);

		// No key manager at all.
		let none = options(dir.path(), None);
		assert_eq!(encryption_error(VLog::new(none)), EncryptionError::NotConfigured);
		assert_eq!(snapshot(&opts.vlog_dir()), before);

		// The key the files name was retired.
		let km = Arc::new(SoftwareKeyManager::new([0x11; 32]));
		km.add_key(2, [0x22; 32]).unwrap();
		km.set_active_key_id(2).unwrap();
		km.retire_key(1).unwrap();
		let retired = Arc::new(EncryptionContext::new(km, db_id(), suite));
		assert_eq!(
			encryption_error(VLog::new(options(dir.path(), Some(&retired)))),
			EncryptionError::KeyRetired
		);
		assert_eq!(snapshot(&opts.vlog_dir()), before);

		// The right key still opens everything: nothing was lost on the way.
		VLog::new(Arc::clone(&opts)).unwrap();
		assert_eq!(snapshot(&opts.vlog_dir()), before);
	}
}

#[test]
fn a_key_id_the_manager_has_never_held_is_named() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	// The files are sealed under key 2.
	let km = Arc::new(SoftwareKeyManager::new([0x11; 32]));
	km.add_key(2, [0x22; 32]).unwrap();
	km.set_active_key_id(2).unwrap();
	let writer = Arc::new(EncryptionContext::new(km, db_id(), CipherSuite::Aes256Gcm));
	let opts = options(dir.path(), Some(&writer));
	make_file(&opts, &writer, 1, &[(b"key", b"value")]);
	let before = snapshot(&opts.vlog_dir());

	// A manager that only has key 1.
	let reader = context(CipherSuite::Aes256Gcm);
	assert_eq!(
		encryption_error(VLog::new(options(dir.path(), Some(&reader)))),
		EncryptionError::UnknownKeyId
	);
	assert_eq!(snapshot(&opts.vlog_dir()), before);
	VLog::new(opts).unwrap();
}

#[test]
fn a_log_reads_the_kind_of_file_it_is_configured_for_and_refuses_the_other() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let ctx = context(suite);

		// A plaintext file in a log that is configured with encryption, whether it holds an entry
		// or only its header.
		for entries in [0, 1] {
			let dir = TempDir::new().unwrap();
			let opts = options(dir.path(), None);
			let mut writer = VLogWriter::new(&opts.vlog_file_path(1), 1, 1 << 20, 0).unwrap();
			for _ in 0..entries {
				writer.append(b"key", b"value").unwrap();
			}
			writer.flush().unwrap();
			drop(writer);
			let before = snapshot(&opts.vlog_dir());

			let sealed_opts = options(dir.path(), Some(&ctx));
			assert_eq!(
				encryption_error(VLog::new(sealed_opts)),
				EncryptionError::PlaintextInEncryptedDb,
				"{suite:?}, {entries} entries"
			);
			assert_eq!(snapshot(&opts.vlog_dir()), before, "it is not set aside, nor repaired");
		}

		// A directory with one of each: the plaintext file is the one that is refused when the
		// log is sealed, and the sealed file is when it is not.
		let dir = TempDir::new().unwrap();
		let plain = options(dir.path(), None);
		let mut writer = VLogWriter::new(&plain.vlog_file_path(1), 1, 1 << 20, 0).unwrap();
		writer.append(b"key", b"value").unwrap();
		writer.flush().unwrap();
		drop(writer);
		let sealed_opts = options(dir.path(), Some(&ctx));
		make_file(&sealed_opts, &ctx, 2, &[(b"key", b"value")]);
		let before = snapshot(&plain.vlog_dir());
		assert_eq!(
			encryption_error(VLog::new(Arc::clone(&sealed_opts))),
			EncryptionError::PlaintextInEncryptedDb
		);
		assert_eq!(encryption_error(VLog::new(plain)), EncryptionError::NotConfigured);
		assert_eq!(snapshot(&sealed_opts.vlog_dir()), before);
	}
}

#[test]
fn a_file_that_turns_up_after_the_log_was_opened_is_held_to_the_same_rule() {
	if tfs::skip_listed() {
		return;
	}
	let ctx = context(CipherSuite::Aes256Gcm);

	// A sealed log meets a plaintext file at a read.
	let dir = TempDir::new().unwrap();
	let sealed_opts = options(dir.path(), Some(&ctx));
	let vlog = VLog::new(Arc::clone(&sealed_opts)).unwrap();
	let mut writer = VLogWriter::new(&sealed_opts.vlog_file_path(5), 5, 1 << 20, 0).unwrap();
	let plain_pointer = writer.append(b"key", b"value").unwrap();
	writer.flush().unwrap();
	drop(writer);
	assert_eq!(encryption_error(vlog.get(&plain_pointer)), EncryptionError::PlaintextInEncryptedDb);

	// A plaintext log meets a sealed file.
	let dir = TempDir::new().unwrap();
	let plain_opts = options(dir.path(), None);
	let vlog = VLog::new(Arc::clone(&plain_opts)).unwrap();
	let sealed_pointers = make_file(&plain_opts, &ctx, 5, &[(b"key", b"value")]);
	assert_eq!(encryption_error(vlog.get(&sealed_pointers[0])), EncryptionError::NotConfigured);
}

#[test]
fn a_pointer_and_a_file_that_disagree_about_being_sealed_read_nothing() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::XChaCha20Poly1305);
	let opts = options(dir.path(), Some(&ctx));
	let sealed = make_file(&opts, &ctx, 1, &[(b"key", b"value")]);
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	// The pointer of a plaintext entry into a sealed file.
	let plain = ValuePointer::new(1, sealed[0].offset, 3, 5, sealed[0].checksum);
	assert!(is_corruption(vlog.get(&plain)));

	// The pointer of a sealed entry into a plaintext file.
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path(), None);
	let mut writer = VLogWriter::new(&opts.vlog_file_path(1), 1, 1 << 20, 0).unwrap();
	let pointer = writer.append(b"key", b"value").unwrap();
	writer.flush().unwrap();
	drop(writer);
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	// The pointer that lies comes first. A value that a read has put in the cache is found by the
	// file and the offset alone, so once the honest pointer had been read, the lying one would be
	// answered from the cache and would never reach the file.
	let sealed = ValuePointer::sealed(1, pointer.offset, 3, 5, pointer.checksum);
	assert!(is_corruption(vlog.get(&sealed)));
	assert_eq!(vlog.get(&pointer).unwrap(), b"value");
}

#[test]
fn a_pointer_with_absurd_sizes_or_offset_is_an_error_and_allocates_nothing_for_them() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(dir.path(), Some(&ctx));
	let pointers = make_file(&opts, &ctx, 1, &[(b"key", b"value")]);
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	let honest = &pointers[0];

	for (offset, key_size, value_size) in [
		(honest.offset, u32::MAX, u32::MAX),
		(honest.offset, 3, u32::MAX),
		(honest.offset, 3, 6),
		(honest.offset + 1, 3, 5),
		(u64::MAX, 3, 5),
		(u64::MAX - 10, 3, 5),
		(1 << 40, 3, 5),
	] {
		let lying = ValuePointer {
			offset,
			key_size,
			value_size,
			..honest.clone()
		};
		assert!(
			is_corruption(vlog.get(&lying)),
			"offset {offset}, key {key_size}, value {value_size}"
		);
	}
	// A file that does not exist is the existing error for it, not a panic.
	let elsewhere = ValuePointer {
		file_id: 77,
		..honest.clone()
	};
	assert!(vlog.get(&elsewhere).is_err());
	assert_eq!(vlog.get(honest).unwrap(), b"value");
}

#[test]
fn an_entry_that_was_moved_or_copied_between_positions_and_files_does_not_open() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let opts = options(dir.path(), Some(&ctx));
		// Entries of one size, so that one fits exactly where another is.
		let entries: [(&[u8], &[u8]); 2] = [(b"key-0", b"value-0"), (b"key-1", b"value-1")];
		let one = make_file(&opts, &ctx, 1, &entries);
		let two = make_file(&opts, &ctx, 2, &entries);
		let size = one[0].total_entry_size() as usize;
		assert_eq!(one[1].total_entry_size() as usize, size);

		// The first entry of file 1 over the second: the same bytes, at another position.
		let path_one = opts.vlog_file_path(1);
		let mut bytes = std::fs::read(&path_one).unwrap();
		let first = bytes[one[0].offset as usize..][..size].to_vec();
		bytes[one[1].offset as usize..][..size].copy_from_slice(&first);
		std::fs::write(&path_one, &bytes).unwrap();

		// The second entry of file 2, bytes and CRC of file 1's first entry: another file, and
		// another position.
		let path_two = opts.vlog_file_path(2);
		let mut bytes = std::fs::read(&path_two).unwrap();
		bytes[two[1].offset as usize..][..size].copy_from_slice(&first);
		std::fs::write(&path_two, &bytes).unwrap();
		// The same position in the other file, which has another key.
		bytes[two[0].offset as usize..][..size].copy_from_slice(&first);
		std::fs::write(&path_two, &bytes).unwrap();

		let vlog = VLog::new(Arc::clone(&opts)).unwrap();
		assert_eq!(vlog.get(&one[0]).unwrap(), b"value-0", "the entry that was copied is intact");
		for (pointer, path) in [(&one[1], &path_one), (&two[1], &path_two), (&two[0], &path_two)] {
			assert!(is_corruption(vlog.get(pointer)), "{suite:?}: the CRC is not the pointer's");
			assert_eq!(
				encryption_error(vlog.get(&with_crc_of(pointer, path))),
				EncryptionError::AuthenticationFailed,
				"{suite:?}, {pointer:?}"
			);
		}
	}
}

#[test]
fn a_file_under_another_id_does_not_open() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(dir.path(), Some(&ctx));
	make_file(&opts, &ctx, 1, &[(b"key", b"value")]);

	// Renamed: the id in its header is not its name.
	std::fs::rename(opts.vlog_file_path(1), opts.vlog_file_path(2)).unwrap();
	let before = snapshot(&opts.vlog_dir());
	assert!(is_corruption(VLog::new(Arc::clone(&opts))));
	assert_eq!(snapshot(&opts.vlog_dir()), before);

	// Renamed, and the file header and its checksum rewritten to say so: the encryption header
	// still names the file that it was made for.
	let path = opts.vlog_file_path(2);
	let mut bytes = std::fs::read(&path).unwrap();
	bytes[6..10].copy_from_slice(&2u32.to_be_bytes());
	fix_file_crc(&mut bytes);
	std::fs::write(&path, &bytes).unwrap();
	let before = snapshot(&opts.vlog_dir());
	assert_eq!(encryption_error(VLog::new(Arc::clone(&opts))), EncryptionError::FileIdMismatch);
	assert_eq!(snapshot(&opts.vlog_dir()), before);
}

#[test]
fn a_file_that_a_crash_left_without_its_header_is_set_aside_and_never_deleted() {
	if tfs::skip_listed() {
		return;
	}
	let ctx = context(CipherSuite::Aes256Gcm);
	let header = {
		let dir = TempDir::new().unwrap();
		let opts = options(dir.path(), Some(&ctx));
		make_file(&opts, &ctx, 1, &[]);
		std::fs::read(opts.vlog_file_path(1)).unwrap()
	};
	assert_eq!(header.len(), 127);
	let mut bad_crc = header.clone();
	bad_crc[100] ^= 0x01;
	let mut zero_tail = header.clone();
	zero_tail[60..].fill(0);

	let torn: [(&str, Vec<u8>); 6] = [
		("empty", Vec::new()),
		("a part of the header", header[..60].to_vec()),
		("the magic", header[..4].to_vec()),
		("a header with a bad checksum", bad_crc),
		("a header that was only partly written", zero_tail),
		("zeros", vec![0; 127]),
	];
	for (what, bytes) in torn {
		let dir = TempDir::new().unwrap();
		let opts = options(dir.path(), Some(&ctx));
		std::fs::write(opts.vlog_file_path(1), &bytes).unwrap();

		let vlog = VLog::new(Arc::clone(&opts)).unwrap();
		let names: Vec<_> = snapshot(&opts.vlog_dir()).into_iter().collect();
		assert_eq!(
			names,
			vec![("00000000000000000001.vlog.torn".to_string(), bytes.clone())],
			"{what}: set aside, with its bytes"
		);
		assert!(vlog.file_handles.read().is_empty(), "{what}");
		assert!(vlog.writer.read().is_none(), "{what}");
		// Its id is not used again.
		assert_eq!(vlog.next_file_id.load(Ordering::SeqCst), 2, "{what}");
		assert_eq!(vlog.active_writer_id.load(Ordering::SeqCst), 1, "{what}");
	}
}

#[test]
fn only_the_newest_file_can_be_set_aside_and_only_for_a_header_that_was_not_completed() {
	if tfs::skip_listed() {
		return;
	}
	let ctx = context(CipherSuite::XChaCha20Poly1305);

	// An older file with entries and a newer one that was cut off: the older one is read, the
	// newer one is set aside and its id stays taken.
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path(), Some(&ctx));
	let pointers = make_file(&opts, &ctx, 1, &[(b"key", b"value")]);
	let header = std::fs::read(opts.vlog_file_path(1)).unwrap()[..127].to_vec();
	std::fs::write(opts.vlog_file_path(2), &header[..50]).unwrap();
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	assert_eq!(vlog.get(&pointers[0]).unwrap(), b"value");
	assert_eq!(vlog.next_file_id.load(Ordering::SeqCst), 3);
	assert!(opts.vlog_dir().join("00000000000000000002.vlog.torn").exists());
	assert!(!opts.vlog_file_path(2).exists());
	assert!(opts.vlog_file_path(1).exists());
	drop(vlog);

	// A file cut off that is not the newest is an error, and is left where it is.
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path(), Some(&ctx));
	make_file(&opts, &ctx, 2, &[(b"key", b"value")]);
	std::fs::write(opts.vlog_file_path(1), &header[..50]).unwrap();
	let before = snapshot(&opts.vlog_dir());
	assert_eq!(encryption_error(VLog::new(Arc::clone(&opts))), EncryptionError::MalformedHeader);
	assert_eq!(snapshot(&opts.vlog_dir()), before);

	// An empty file that is not the newest.
	std::fs::write(opts.vlog_file_path(1), b"").unwrap();
	let before = snapshot(&opts.vlog_dir());
	assert!(is_corruption(VLog::new(Arc::clone(&opts))));
	assert_eq!(snapshot(&opts.vlog_dir()), before);

	// A newest file that is damaged and is longer than a header may hold entries: an error.
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path(), Some(&ctx));
	make_file(&opts, &ctx, 1, &[(b"key", b"value")]);
	let mut bytes = std::fs::read(opts.vlog_file_path(1)).unwrap();
	bytes[100] ^= 0x01;
	std::fs::write(opts.vlog_file_path(1), &bytes).unwrap();
	let before = snapshot(&opts.vlog_dir());
	assert_eq!(encryption_error(VLog::new(Arc::clone(&opts))), EncryptionError::MalformedHeader);
	assert_eq!(snapshot(&opts.vlog_dir()), before);

	// A header that is whole and is only for another key is not torn, however short the file.
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path(), Some(&ctx));
	make_file(&opts, &ctx, 1, &[]);
	let before = snapshot(&opts.vlog_dir());
	let wrong = options(dir.path(), Some(&context_with_root(CipherSuite::XChaCha20Poly1305, 0x42)));
	assert_eq!(encryption_error(VLog::new(wrong)), EncryptionError::WrongKey);
	assert_eq!(snapshot(&opts.vlog_dir()), before);
	// And with the right key it is a file like any other, which is read and not appended to.
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	assert_eq!(vlog.file_handles.read().len(), 1);
	assert!(vlog.writer.read().is_none());
	assert_eq!(snapshot(&opts.vlog_dir()), before);
}

/// The directory lists its files in an order of its own, so a log that renames the file a crash cut
/// off as soon as it reaches it renames it before a key that does not fit has been refused, or not,
/// depending on where the file system puts it. Whichever it is, a log that is refused changes
/// nothing, and the file is set aside once the log has opened.
#[test]
fn a_torn_file_is_set_aside_only_when_every_other_file_has_opened() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let ctx = context(suite);
		let other_db = Arc::new(EncryptionContext::new(
			Arc::new(SoftwareKeyManager::new([0x11; 32])),
			DbId::from_bytes([0xD1; 16]),
			suite,
		));
		let km = Arc::new(SoftwareKeyManager::new([0x11; 32]));
		km.add_key(2, [0x22; 32]).unwrap();
		km.set_active_key_id(2).unwrap();
		km.retire_key(1).unwrap();
		let retired = Arc::new(EncryptionContext::new(km, db_id(), suite));

		// Nothing, and a part of a header: each is torn when it is the newest file.
		for torn_len in [0, 50] {
			let dir = TempDir::new().unwrap();
			let opts = options(dir.path(), Some(&ctx));
			let pointers = make_file(&opts, &ctx, 1, &[(b"key", b"value")]);
			let header = std::fs::read(opts.vlog_file_path(1)).unwrap()[..127].to_vec();
			std::fs::write(opts.vlog_file_path(2), &header[..torn_len]).unwrap();
			let before = snapshot(&opts.vlog_dir());

			let refused = [
				(Some(context_with_root(suite, 0x99)), EncryptionError::WrongKey),
				(Some(Arc::clone(&other_db)), EncryptionError::DbIdMismatch),
				(Some(Arc::clone(&retired)), EncryptionError::KeyRetired),
				(None, EncryptionError::NotConfigured),
			];
			for (refusing, expected) in refused {
				let result = VLog::new(options(dir.path(), refusing.as_ref()));
				assert_eq!(encryption_error(result), expected, "{suite:?}, {torn_len} bytes");
				assert_eq!(
					snapshot(&opts.vlog_dir()),
					before,
					"{suite:?}, {torn_len} bytes: a log that was refused changed the directory"
				);
			}

			// Opened with the key of the files, the other file is read and the torn one is set
			// aside.
			let vlog = VLog::new(Arc::clone(&opts)).unwrap();
			assert_eq!(vlog.get(&pointers[0]).unwrap(), b"value");
			assert_eq!(vlog.next_file_id.load(Ordering::SeqCst), 3);
			let after = snapshot(&opts.vlog_dir());
			assert_eq!(
				after.get("00000000000000000002.vlog.torn"),
				Some(&header[..torn_len].to_vec()),
				"{suite:?}, {torn_len} bytes: set aside with its bytes"
			);
			assert_eq!(after.len(), 2, "the file with an entry and the file that was set aside");
		}
	}
}

#[test]
fn a_name_the_log_set_aside_is_not_read_as_a_file() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(dir.path(), Some(&ctx));
	let pointers = make_file(&opts, &ctx, 3, &[(b"key", b"value")]);
	std::fs::write(opts.vlog_dir().join("00000000000000000009.vlog.torn"), b"junk").unwrap();
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	assert_eq!(vlog.get(&pointers[0]).unwrap(), b"value");
	assert_eq!(vlog.next_file_id.load(Ordering::SeqCst), 4);
}

#[test]
fn a_plaintext_log_is_left_as_it_was_found() {
	if tfs::skip_listed() {
		return;
	}
	// An empty newest file in a plaintext log is reused as it always was, and is not set aside.
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path(), None);
	std::fs::write(opts.vlog_file_path(1), b"").unwrap();
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	let names: Vec<_> = snapshot(&opts.vlog_dir()).into_keys().collect();
	assert_eq!(names, vec!["00000000000000000001.vlog".to_string()]);
	assert_eq!(std::fs::metadata(opts.vlog_file_path(1)).unwrap().len(), 31, "its header");
	assert!(vlog.writer.read().is_some(), "a plaintext file is reopened for appending");
}

#[test]
fn a_restore_makes_the_reads_of_a_sealed_file_id_go_to_the_restored_file() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let opts = options(dir.path(), Some(&ctx));
		let old = make_file(&opts, &ctx, 1, &[(b"key", b"the old value")]);
		let vlog = VLog::new(Arc::clone(&opts)).unwrap();
		assert_eq!(vlog.get(&old[0]).unwrap(), b"the old value", "reads, and caches, the old file");

		// The restored directory has a file 1 of its own, sealed under its own salt, with an entry
		// at the same offset.
		let held = vlog.hold_writer();
		std::fs::remove_file(opts.vlog_file_path(1)).unwrap();
		let restored = make_file(&opts, &ctx, 1, &[(b"key", b"the restored value")]);
		assert_eq!(restored[0].offset, old[0].offset);
		vlog.reload(held).unwrap();

		assert_eq!(vlog.get(&restored[0]).unwrap(), b"the restored value");
		assert_ne!(
			vlog.get(&old[0]).ok(),
			Some(b"the old value".to_vec()),
			"{suite:?}: the value or the cipher of the old file 1 was used after the reload"
		);
		assert_eq!(vlog.file_handles.read().len(), 1);
		assert_eq!(vlog.next_file_id.load(Ordering::SeqCst), 2);

		// A restored directory of the other kind is refused as it is when the log is opened.
		let held = vlog.hold_writer();
		std::fs::remove_file(opts.vlog_file_path(1)).unwrap();
		let mut writer = VLogWriter::new(&opts.vlog_file_path(1), 1, 1 << 20, 0).unwrap();
		writer.append(b"key", b"value").unwrap();
		writer.flush().unwrap();
		drop(writer);
		assert_eq!(encryption_error(vlog.reload(held)), EncryptionError::PlaintextInEncryptedDb);
	}
}

#[test]
fn a_key_manager_without_a_resolved_context_is_refused_instead_of_writing_plaintext() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let mut opts = (*options(dir.path(), None)).clone();
	opts.key_manager = Some(Arc::new(SoftwareKeyManager::new([0x11; 32])));
	match VLog::new(Arc::new(opts.clone())) {
		Err(Error::InvalidArgument(message)) => {
			assert!(message.contains("key manager"), "{message}");
		}
		Err(other) => panic!("expected an invalid argument, got {other:?}"),
		Ok(_) => panic!("a log without a context was opened beside a key manager"),
	}
	assert!(snapshot(&opts.vlog_dir()).is_empty());

	// With the context it is the log of an encrypted database.
	opts.encryption_context = Some(context(CipherSuite::Aes256Gcm));
	VLog::new(Arc::new(opts)).unwrap();
}

/// The salt of a sealed file: bytes 75 to 107, in the encryption header after the file header
/// and its checksum.
fn salt_of(path: &Path) -> Vec<u8> {
	std::fs::read(path).unwrap()[75..107].to_vec()
}

/// The key id of a sealed file: bytes 43 to 47.
fn key_id_of(path: &Path) -> u32 {
	let bytes = std::fs::read(path).unwrap();
	u32::from_be_bytes(bytes[43..47].try_into().unwrap())
}

/// Options with a small file size, so that a few entries fill a file.
fn small_files(dir: &Path, ctx: &Arc<EncryptionContext>, max_file_size: u64) -> Arc<Options> {
	let mut opts = (*options(dir, Some(ctx))).clone();
	opts.vlog_max_file_size = max_file_size;
	Arc::new(opts)
}

#[test]
fn appended_values_read_back_across_rollovers_and_restarts() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let opts = small_files(dir.path(), &ctx, 300);
		let vlog = VLog::new(Arc::clone(&opts)).unwrap();

		// Keys and values of every size that matters: empty ones, one byte, and ones that fill a
		// file by themselves.
		let shapes: [(&[u8], usize); 10] = [
			(b"k0", 10),
			(b"", 0),
			(b"empty-value", 0),
			(b"", 20),
			(b"k4", 300),
			(b"k5", 1),
			(b"key-6", 150),
			(&[0xCD; 40], 5),
			(b"k8", 400),
			(b"k9", 7),
		];
		let mut expected = Vec::new();
		for (i, (key, len)) in shapes.iter().enumerate() {
			let value = value_of(i as u8, *len);
			let pointer = vlog.append(key, &value).unwrap();
			assert!(pointer.is_sealed(), "{suite:?}, entry {i}");
			expected.push((pointer, value));
		}
		vlog.sync().unwrap();

		// Several files, numbered from 1, each with its first entry where the header ends and a
		// salt of its own.
		let files: BTreeSet<u32> = expected.iter().map(|(pointer, _)| pointer.file_id).collect();
		assert!(files.len() >= 3, "{suite:?}: {files:?}");
		assert_eq!(
			files.iter().copied().collect::<Vec<_>>(),
			(1..=files.len() as u32).collect::<Vec<_>>()
		);
		let mut salts = BTreeSet::new();
		for file in &files {
			let path = opts.vlog_file_path(u64::from(*file));
			assert_eq!(std::fs::read(&path).unwrap()[..4], *b"VLOG");
			salts.insert(salt_of(&path));
			let first = expected.iter().find(|(pointer, _)| pointer.file_id == *file).unwrap();
			assert_eq!(first.0.offset, 127, "file {file}");
		}
		assert_eq!(salts.len(), files.len(), "no two files share a salt");
		for (pointer, value) in &expected {
			assert_eq!(&vlog.get(pointer).unwrap(), value, "{suite:?}, {pointer:?}");
		}
		vlog.close().unwrap();
		drop(vlog);

		// A restart reads all of it, through a cache of its own.
		let reopened = VLog::new(options(dir.path(), Some(&ctx))).unwrap();
		for (pointer, value) in &expected {
			assert_eq!(&reopened.get(pointer).unwrap(), value, "{suite:?}, {pointer:?}");
		}
	}
}

#[test]
fn a_new_sealed_file_has_its_header_and_its_name_on_disk_before_its_first_entry() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(dir.path(), Some(&ctx));
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	let directory_syncs = vlog.dir_syncs();

	let pointer = vlog.append(b"key", b"value").unwrap();
	assert_eq!(pointer.offset, 127);
	assert!(vlog.dir_syncs() > directory_syncs, "the directory was fsynced with the creation");
	let path = opts.vlog_file_path(1);
	assert!(
		SYNCED_VLOG_LENGTHS.lock().contains(&(path.clone(), 127)),
		"the file was fsynced with nothing in it but its header"
	);
	// Nothing was written yet that an fsync would not already have covered: the entry is in the
	// buffer, and the file is the header.
	assert_eq!(std::fs::metadata(&path).unwrap().len(), 127);

	let directory_syncs = vlog.dir_syncs();
	vlog.sync().unwrap();
	assert_eq!(vlog.dir_syncs(), directory_syncs, "a sync has no creation left to cover");
	assert_eq!(vlog.get(&pointer).unwrap(), b"value");
}

#[test]
fn a_failed_append_poisons_the_file_and_the_next_append_goes_to_a_file_of_its_own() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let opts = options(dir.path(), Some(&ctx));
		let vlog = VLog::new(Arc::clone(&opts)).unwrap();
		let first = vlog.append(b"key-1", &value_of(1, 100)).unwrap();
		vlog.flush().unwrap();
		let path_one = opts.vlog_file_path(1);

		// The file takes 100 more bytes and fails: an entry that is larger than the buffer is
		// written straight to it, so 100 bytes of it reach the file.
		vlog.fail_writes_after(100);
		let failed = vlog.append(b"key-2", &value_of(2, 20_000));
		assert!(matches!(failed, Err(Error::Io(_))), "{failed:?}");
		let torn = std::fs::read(&path_one).unwrap();
		assert_eq!(torn.len() as u64, 127 + first.total_entry_size() + 100);

		// The next append is not made at the position that failed, nor anywhere in that file:
		// it starts a file, and a key, of its own.
		let third = vlog.append(b"key-3", &value_of(3, 100)).unwrap();
		vlog.sync().unwrap();
		assert_eq!(third.file_id, 2);
		assert_eq!(third.offset, 127, "positions start over under a key that is new");
		assert_eq!(
			std::fs::read(&path_one).unwrap(),
			torn,
			"{suite:?}: the failed file was written to"
		);
		assert_ne!(salt_of(&path_one), salt_of(&opts.vlog_file_path(2)));

		// What was appended before the failure and after it reads back.
		assert_eq!(vlog.get(&first).unwrap(), value_of(1, 100));
		assert_eq!(vlog.get(&third).unwrap(), value_of(3, 100));
		vlog.close().unwrap();
		drop(vlog);

		// And after a restart, which leaves the files as they are and starts a third.
		let reopened = VLog::new(options(dir.path(), Some(&ctx))).unwrap();
		assert_eq!(reopened.get(&first).unwrap(), value_of(1, 100));
		assert_eq!(reopened.get(&third).unwrap(), value_of(3, 100));
		assert_eq!(std::fs::read(&path_one).unwrap(), torn);
	}
}

/// A write that fails wherever it fails, in the entry that is being written or in the flush of the
/// ones before it, never has the same position sealed twice: what was appended is in the file
/// intact, the failed entry has no pointer, and the writer takes nothing more.
#[test]
fn a_write_that_fails_anywhere_never_gives_a_position_out_twice() {
	// (the length of the second value, the points at which the writes fail). The first entry is
	// 75 bytes: its length, "key-1", 50 bytes and the tag. A large second entry is written
	// straight to the file and fails in its append, a small one waits in the buffer and fails
	// in the flush. The last point of each is exactly as many bytes as the writes take.
	let shapes: [(usize, Vec<usize>); 2] = [
		(12_000, vec![0, 1, 74, 75, 76, 175, 12_099, 12_100]),
		(300, vec![0, 1, 74, 75, 76, 399, 400]),
	];
	let (mut failed_appends, mut failed_flushes) = (0, 0);
	for suite in SUITES {
		let ctx = context(suite);
		for (second_len, points) in &shapes {
			for point in points {
				let dir = TempDir::new().unwrap();
				let path = dir.path().join("sealed.vlog");
				let mut writer = VLogWriter::create_sealed(&path, 1, 1 << 30, 0, &ctx).unwrap();
				let (v1, v2, v3) = (value_of(1, 50), value_of(2, *second_len), value_of(3, 30));
				let first = writer.append(b"key-1", &v1).unwrap();
				assert_eq!(first.total_entry_size(), 75);
				let offset2 = first.offset + 75;
				let stored2 = 4 + 5 + *second_len as u64 + 16;

				writer.fail_writes_after(*point);
				let mut intact = vec![(first, v1)];
				match writer.append(b"key-2", &v2) {
					Ok(second) => {
						assert_eq!(second.offset, offset2);
						intact.push((second, v2));
						let third = writer.append(b"key-3", &v3).unwrap();
						assert_eq!(third.offset, offset2 + stored2);
						intact.push((third, v3.clone()));
					}
					Err(_) => {
						failed_appends += 1;
						// The position was spoken for before a byte was written: the cipher will
						// not seal it or any before it again, and the writer's own offset is past
						// the entry, so nothing can be appended at either.
						assert_eq!(writer.next_position_to_seal(), Some(offset2 + 1));
						assert_eq!(writer.current_offset, offset2 + stored2);
						assert!(writer.append(b"key-3", &v3).is_err(), "a poisoned writer");
					}
				}

				// Whatever is still buffered reaches the file, and a failed flush is retried.
				if writer.flush().is_err() {
					failed_flushes += 1;
					writer.flush().unwrap();
				}
				let length = std::fs::metadata(&path).unwrap().len();
				if intact.len() == 1 {
					assert!(
						(offset2..=offset2 + stored2).contains(&length),
						"point {point}: the file ends inside the failed entry, not past it: {length}"
					);
					assert!(writer.append(b"key-4", &v3).is_err());
					writer.flush().unwrap();
					assert_eq!(
						std::fs::metadata(&path).unwrap().len(),
						length,
						"point {point}: the failed file was written to"
					);
				} else {
					assert_eq!(length, offset2 + stored2 + 4 + 5 + 30 + 16);
				}

				// Every pointer that was handed out opens in the file as it is.
				let bytes = std::fs::read(&path).unwrap();
				let reader = sealed::open_header(&ctx, 1, &bytes[..127], false).unwrap();
				for (pointer, value) in &intact {
					let start = pointer.offset as usize;
					let stored = bytes[start..start + pointer.total_entry_size() as usize].to_vec();
					assert_eq!(
						&sealed::open_entry(&reader, pointer, stored).unwrap(),
						value,
						"{suite:?}, point {point}, {pointer:?}"
					);
				}
			}
		}
	}
	assert!(failed_appends >= 10, "the sweep must fail appends: {failed_appends}");
	assert!(failed_flushes >= 4, "and flushes: {failed_flushes}");
}

#[test]
fn a_writer_that_is_asked_to_seal_a_position_again_is_refused_and_poisoned() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let path = dir.path().join("sealed.vlog");
		let mut writer = VLogWriter::create_sealed(&path, 1, 1 << 30, 0, &ctx).unwrap();
		let first = writer.append(b"a", b"1").unwrap();
		let second = writer.append(b"b", b"2").unwrap();
		assert_eq!(second.offset, first.offset + first.total_entry_size());
		writer.flush().unwrap();
		let length = std::fs::metadata(&path).unwrap().len();

		// A bug that put the offset back to where the first entry is: the cipher is the backstop.
		writer.current_offset = first.offset;
		assert_eq!(
			encryption_error(writer.append(b"c", b"3")),
			EncryptionError::PositionReuse,
			"{suite:?}"
		);
		assert!(writer.append(b"d", b"4").is_err(), "the writer is poisoned");
		writer.flush().unwrap();
		assert_eq!(std::fs::metadata(&path).unwrap().len(), length, "nothing was written");
	}
}

#[test]
fn a_restart_never_reopens_a_sealed_file_and_starts_a_new_one_with_a_new_salt() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let mut expected = Vec::new();
		let mut salts = BTreeSet::new();
		for run in 1..=3u32 {
			let opts = options(dir.path(), Some(&ctx));
			let vlog = VLog::new(Arc::clone(&opts)).unwrap();
			assert!(vlog.writer.read().is_none(), "run {run}: a sealed file is not reopened");
			assert_eq!(vlog.next_file_id.load(Ordering::SeqCst), run);
			let before = snapshot(&opts.vlog_dir());

			let value = value_of(run as u8, 40 * run as usize);
			let pointer = vlog.append(b"key", &value).unwrap();
			assert_eq!(pointer.file_id, run, "each run starts the next file");
			assert_eq!(pointer.offset, 127);
			expected.push((pointer, value));
			vlog.close().unwrap();

			// The files of the runs before are as they were, byte for byte.
			let after = snapshot(&opts.vlog_dir());
			for (name, bytes) in &before {
				assert_eq!(after.get(name), Some(bytes), "run {run}: {name}");
			}
			assert!(salts.insert(salt_of(&opts.vlog_file_path(u64::from(run)))));
		}

		let vlog = VLog::new(options(dir.path(), Some(&ctx))).unwrap();
		for (pointer, value) in &expected {
			assert_eq!(&vlog.get(pointer).unwrap(), value, "{suite:?}, {pointer:?}");
		}
	}
}

#[test]
fn a_sealed_file_is_never_reopened_for_appending() {
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::XChaCha20Poly1305);
	let path = dir.path().join("00000000000000000001.vlog");
	for entries in [0, 2] {
		let _ = std::fs::remove_file(&path);
		let mut writer = VLogWriter::create_sealed(&path, 1, 1 << 20, 0, &ctx).unwrap();
		for _ in 0..entries {
			writer.append(b"key", b"value").unwrap();
		}
		writer.flush().unwrap();
		drop(writer);
		let before = std::fs::read(&path).unwrap();

		// The writer of plaintext files refuses it, and so does the one of sealed files.
		match VLogWriter::new(&path, 1, 1 << 20, 0) {
			Err(Error::Encryption(e)) => assert!(matches!(e, EncryptionError::InvalidInput(_))),
			Err(other) => panic!("expected an encryption error, got {other:?}"),
			Ok(_) => panic!("a sealed file was reopened for appending"),
		}
		match VLogWriter::create_sealed(&path, 1, 1 << 20, 0, &ctx) {
			Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists),
			Err(other) => panic!("expected an I/O error, got {other:?}"),
			Ok(_) => panic!("an existing file was created again"),
		}
		assert_eq!(std::fs::read(&path).unwrap(), before, "{entries} entries");
	}
}

#[test]
fn a_file_that_is_in_the_way_is_an_error_and_is_never_appended_to() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(dir.path(), Some(&ctx));
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();

	// A file that the log did not know of takes the id that its next file would have.
	std::fs::write(opts.vlog_file_path(1), b"stale").unwrap();
	match vlog.append(b"key", b"value") {
		Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists),
		other => panic!("expected an I/O error, got {other:?}"),
	}
	assert_eq!(std::fs::read(opts.vlog_file_path(1)).unwrap(), b"stale");

	// The id was used up: the next append is to another file.
	let pointer = vlog.append(b"key", b"value").unwrap();
	assert_eq!(pointer.file_id, 2);
	vlog.flush().unwrap();
	assert_eq!(vlog.get(&pointer).unwrap(), b"value");
	let names: Vec<_> = snapshot(&opts.vlog_dir()).into_keys().collect();
	assert_eq!(names, ["00000000000000000001.vlog", "00000000000000000002.vlog"]);
}

/// A key manager whose provider can be made unavailable.
struct Flaky {
	fail: AtomicBool,
}

impl KeyManager for Flaky {
	fn active_key_id(&self) -> u32 {
		1
	}

	fn key_state(&self, _key_id: u32) -> KeyState {
		KeyState::Active
	}

	fn root_key(&self, _key_id: u32) -> Result<Zeroizing<[u8; 32]>, EncryptionError> {
		if self.fail.load(Ordering::SeqCst) {
			Err(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable))
		} else {
			Ok(Zeroizing::new([0x33; 32]))
		}
	}
}

#[test]
fn a_key_provider_that_fails_at_a_rollover_fails_that_append_and_creates_nothing() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let provider = Arc::new(Flaky {
		fail: AtomicBool::new(true),
	});
	let ctx = Arc::new(EncryptionContext::new(
		Arc::<Flaky>::clone(&provider),
		db_id(),
		CipherSuite::XChaCha20Poly1305,
	));
	let opts = options(dir.path(), Some(&ctx));
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();

	let error = encryption_error(vlog.append(b"key", b"value"));
	assert_eq!(error, EncryptionError::KeyProvider(KeyProviderFailure::Unavailable));
	assert!(error.is_retryable());
	assert!(snapshot(&opts.vlog_dir()).is_empty(), "no file, and so no header, was started");
	assert!(vlog.writer.read().is_none());

	// The provider is back: the same append goes through, to a file that has a key.
	provider.fail.store(false, Ordering::SeqCst);
	let pointer = vlog.append(b"key", b"value").unwrap();
	vlog.flush().unwrap();
	assert_eq!(vlog.get(&pointer).unwrap(), b"value");
	assert_eq!(snapshot(&opts.vlog_dir()).len(), 1);
}

#[test]
fn a_rotation_of_the_key_between_two_files_leaves_the_older_one_readable() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let km = Arc::new(SoftwareKeyManager::new([0x11; 32]));
		km.add_key(2, [0x22; 32]).unwrap();
		let ctx =
			Arc::new(EncryptionContext::new(Arc::<SoftwareKeyManager>::clone(&km), db_id(), suite));
		let opts = small_files(dir.path(), &ctx, 200);
		let vlog = VLog::new(Arc::clone(&opts)).unwrap();

		let old = vlog.append(b"key-old", &value_of(1, 150)).unwrap();
		km.set_active_key_id(2).unwrap();
		// The file is full, so this is the first entry of a file that is made after the rotation.
		let new = vlog.append(b"key-new", &value_of(2, 150)).unwrap();
		vlog.sync().unwrap();
		assert_eq!((old.file_id, new.file_id), (1, 2));
		assert_eq!(key_id_of(&opts.vlog_file_path(1)), 1);
		assert_eq!(key_id_of(&opts.vlog_file_path(2)), 2);
		assert_eq!(vlog.get(&old).unwrap(), value_of(1, 150));
		assert_eq!(vlog.get(&new).unwrap(), value_of(2, 150));
		vlog.close().unwrap();
		drop(vlog);

		// A new log opens both, and refuses the files of a key that was retired.
		VLog::new(options(dir.path(), Some(&ctx))).unwrap();
		km.retire_key(1).unwrap();
		let before = snapshot(&opts.vlog_dir());
		assert_eq!(
			encryption_error(VLog::new(options(dir.path(), Some(&ctx)))),
			EncryptionError::KeyRetired
		);
		assert_eq!(snapshot(&opts.vlog_dir()), before);
	}
}

#[test]
fn a_plaintext_log_still_writes_the_bytes_it_always_did() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path(), None);
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	let pointer = vlog.append(b"key", b"value").unwrap();
	vlog.sync().unwrap();
	assert!(!pointer.is_sealed());
	assert_eq!(pointer.encode()[0], 1);
	assert_eq!(pointer.offset, 31);

	let bytes = std::fs::read(opts.vlog_file_path(1)).unwrap();
	// The header: "VLOG", version 1, the file id, the creation time (not compared), the file
	// size, no compression, and zeros.
	assert_eq!(bytes[..10], [0x56, 0x4C, 0x4F, 0x47, 0, 1, 0, 0, 0, 1]);
	assert_eq!(bytes[18..26], (1u64 << 20).to_be_bytes());
	assert_eq!(bytes[26..31], [0, 0, 0, 0, 0]);
	// The entry: both lengths, the key, the value and the CRC-32 of the key and the value.
	let mut entry = vec![0, 0, 0, 3, 0, 0, 0, 5];
	entry.extend_from_slice(b"keyvalue");
	entry.extend_from_slice(&crc32fast::hash(b"keyvalue").to_be_bytes());
	assert_eq!(bytes[31..], entry);
	assert_eq!(pointer.checksum, crc32fast::hash(b"keyvalue"));
}
