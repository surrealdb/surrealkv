//! Sealed value-log files at the level of the log: what is read back, what is refused and with
//! which error, and that nothing is repaired, renamed or deleted because a key does not fit.
//!
//! The files are made by hand with the pieces of the sealed layout, so these tests exercise the
//! reader on its own. The data is kept small: AEAD runs at a few MB/s in the debug profile.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use crate::error::{EncryptionError, Error};
use crate::tde::{CipherSuite, DbId, EncryptionContext, SoftwareKeyManager};
use crate::vlog::sealed::{self, SEALED_HEADER_LEN};
use crate::vlog::{VLog, VLogWriter, ValueLocation, ValuePointer, SYNCED_VLOG_FILES};
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
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();
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

fn synced_under(dir: &Path) -> Vec<PathBuf> {
	SYNCED_VLOG_FILES.lock().iter().filter(|path| path.starts_with(dir)).cloned().collect()
}

#[test]
fn a_sealed_log_reads_back_what_was_sealed() {
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
		assert_eq!(vlog.next_file_id.load(std::sync::atomic::Ordering::SeqCst), 3);
		assert_eq!(vlog.active_writer_id.load(std::sync::atomic::Ordering::SeqCst), 2);
		assert!(vlog.writer.read().is_none(), "a sealed file is not reopened for appending");
		assert_eq!(snapshot(&opts.vlog_dir()), before);
	}
}

#[test]
fn the_newest_sealed_file_is_fsynced_by_the_first_sync() {
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(dir.path(), Some(&ctx));
	make_file(&opts, &ctx, 1, &[(b"k", b"v")]);
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	assert_eq!(vlog.unsynced_file_ids(), vec![1], "a run may have left it unsynced");

	vlog.sync().unwrap();
	assert!(synced_under(dir.path()).contains(&opts.vlog_file_path(1)));
	assert!(vlog.unsynced_file_ids().is_empty());
}

#[test]
fn a_sealed_entry_is_authenticated_whatever_the_checksum_level_says() {
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
	assert_eq!(vlog.get(&pointer).unwrap(), b"value");
	let sealed = ValuePointer::sealed(1, pointer.offset, 3, 5, pointer.checksum);
	assert!(is_corruption(vlog.get(&sealed)));
}

#[test]
fn a_pointer_with_absurd_sizes_or_offset_is_an_error_and_allocates_nothing_for_them() {
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
		assert_eq!(vlog.next_file_id.load(std::sync::atomic::Ordering::SeqCst), 2, "{what}");
		assert_eq!(vlog.active_writer_id.load(std::sync::atomic::Ordering::SeqCst), 1, "{what}");
	}
}

#[test]
fn only_the_newest_file_can_be_set_aside_and_only_for_a_header_that_was_not_completed() {
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
	assert_eq!(vlog.next_file_id.load(std::sync::atomic::Ordering::SeqCst), 3);
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

#[test]
fn a_name_the_log_set_aside_is_not_read_as_a_file() {
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(dir.path(), Some(&ctx));
	let pointers = make_file(&opts, &ctx, 3, &[(b"key", b"value")]);
	std::fs::write(opts.vlog_dir().join("00000000000000000009.vlog.torn"), b"junk").unwrap();
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	assert_eq!(vlog.get(&pointers[0]).unwrap(), b"value");
	assert_eq!(vlog.next_file_id.load(std::sync::atomic::Ordering::SeqCst), 4);
}

#[test]
fn a_plaintext_log_is_left_as_it_was_found() {
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
		assert_eq!(vlog.next_file_id.load(std::sync::atomic::Ordering::SeqCst), 2);

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

#[test]
fn a_sealed_log_does_not_start_a_plaintext_file() {
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(dir.path(), Some(&ctx));
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	// Writing sealed files comes with the writer. Until it does, an append is refused and nothing
	// is created.
	assert!(vlog.append(b"key", b"value").is_err());
	assert!(snapshot(&opts.vlog_dir()).is_empty());
}
