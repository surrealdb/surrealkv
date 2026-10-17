//! What `VLog::check_entry` says of an entry: it is in the log and whole, or it is not, whatever
//! the log is configured to verify when it reads. One case for each way that an entry can fail to
//! be there, for a plaintext file and for a sealed one.
//!
//! The files are on the disk, and the log is made over it by name (`opts.fs`), so that a test can
//! damage a file by hand and the log, which holds a handle of it, sees the change.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use crate::error::{EncryptionError, Error};
use crate::tde::{CipherSuite, DbId, EncryptionContext, SoftwareKeyManager};
use crate::vlog::sealed::{self, SEALED_HEADER_LEN};
use crate::vlog::{VLog, ValuePointer};
use crate::{Options, VLogChecksumLevel};

const LEVELS: [VLogChecksumLevel; 2] = [VLogChecksumLevel::Disabled, VLogChecksumLevel::Full];
const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

/// The length of the header of a plaintext file, where its first entry begins.
const HEADER: usize = 31;
const KEY_LEN: usize = 8;
const VALUE_LEN: usize = 100;
/// What an entry of a plaintext file takes: the two lengths, the key, the value and the checksum.
const ENTRY_LEN: usize = 8 + KEY_LEN + VALUE_LEN + 4;

fn options(
	dir: &Path,
	level: VLogChecksumLevel,
	ctx: Option<&Arc<EncryptionContext>>,
) -> Arc<Options> {
	let mut opts = Options {
		path: dir.to_path_buf(),
		vlog_max_file_size: 1 << 20,
		vlog_checksum_verification: level,
		..Default::default()
	};
	// The files are the disk's, whatever file system the run of the tests uses by default.
	opts.fs = crate::vfs::default_fs();
	opts.encryption_context = ctx.cloned();
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();
	Arc::new(opts)
}

fn key_of(i: usize) -> Vec<u8> {
	format!("key{i:05}").into_bytes()
}

fn value_of(i: usize, len: usize) -> Vec<u8> {
	(0..len).map(|b| (b + i) as u8).collect()
}

/// `check_entry` with a buffer of its own.
fn check(vlog: &VLog, pointer: &ValuePointer) -> crate::Result<bool> {
	vlog.check_entry(pointer, &mut Vec::new())
}

/// A plaintext log with `count` entries in its first file, flushed to the file.
struct Plain {
	_dir: TempDir,
	opts: Arc<Options>,
	vlog: VLog,
	pointers: Vec<ValuePointer>,
	file: PathBuf,
}

impl Plain {
	fn new(level: VLogChecksumLevel, count: usize) -> Self {
		let dir = TempDir::new().unwrap();
		let opts = options(dir.path(), level, None);
		let vlog = VLog::new(Arc::clone(&opts)).unwrap();
		let pointers: Vec<_> =
			(0..count).map(|i| vlog.append(&key_of(i), &value_of(i, VALUE_LEN)).unwrap()).collect();
		vlog.flush().unwrap();
		let file = opts.vlog_file_path(1);
		Self {
			_dir: dir,
			opts,
			vlog,
			pointers,
			file,
		}
	}

	fn bytes(&self) -> Vec<u8> {
		std::fs::read(&self.file).unwrap()
	}

	fn rewrite(&self, bytes: &[u8]) {
		std::fs::write(&self.file, bytes).unwrap();
	}

	fn start(&self, entry: usize) -> usize {
		self.pointers[entry].offset as usize
	}

	fn check_all(&self) -> Vec<bool> {
		self.pointers.iter().map(|pointer| check(&self.vlog, pointer).unwrap()).collect()
	}
}

#[test]
fn an_entry_that_is_whole_is_in_the_log_at_every_checksum_level() {
	for level in LEVELS {
		let log = Plain::new(level, 4);
		assert_eq!(log.pointers[1].offset as usize, HEADER + ENTRY_LEN);
		assert_eq!(log.check_all(), [true; 4], "{level:?}");
	}
}

#[test]
fn an_entry_is_read_into_one_buffer_that_the_next_entry_reuses() {
	let log = Plain::new(VLogChecksumLevel::Full, 0);
	let big = log.vlog.append(b"big", &value_of(1, 5000)).unwrap();
	let small = log.vlog.append(b"small", &value_of(2, 10)).unwrap();
	log.vlog.flush().unwrap();

	let mut buf = Vec::new();
	assert!(log.vlog.check_entry(&big, &mut buf).unwrap());
	let (capacity, address) = (buf.capacity(), buf.as_ptr());
	assert!(capacity >= 5000);
	for _ in 0..3 {
		assert!(log.vlog.check_entry(&small, &mut buf).unwrap());
		assert!(log.vlog.check_entry(&big, &mut buf).unwrap());
	}
	assert_eq!((buf.capacity(), buf.as_ptr()), (capacity, address), "the buffer was not replaced");
}

#[test]
fn a_check_puts_nothing_in_the_block_cache() {
	let log = Plain::new(VLogChecksumLevel::Full, 3);
	for pointer in &log.pointers {
		assert!(check(&log.vlog, pointer).unwrap());
		let cache_id = log.vlog.cache_id(pointer.file_id);
		assert!(
			log.opts.block_cache.get_vlog(cache_id, pointer.offset).is_none(),
			"a check does not cache the value"
		);
	}
	// A read does, so the cache is what the check is not using.
	let pointer = &log.pointers[0];
	log.vlog.get(pointer).unwrap();
	let cache_id = log.vlog.cache_id(pointer.file_id);
	assert!(log.opts.block_cache.get_vlog(cache_id, pointer.offset).is_some());
}

#[test]
fn an_entry_of_a_file_that_is_not_there_is_not_in_the_log() {
	for level in LEVELS {
		let log = Plain::new(level, 2);
		let elsewhere = ValuePointer {
			file_id: 9,
			..log.pointers[0].clone()
		};
		assert!(!check(&log.vlog, &elsewhere).unwrap(), "{level:?}");
	}
}

#[test]
fn an_entry_that_the_end_of_its_file_cuts_short_is_not_in_the_log() {
	for level in LEVELS {
		let log = Plain::new(level, 4);
		let bytes = log.bytes();
		// In the middle of the third entry, and at the end of its header, and one byte short.
		for cut in [
			log.start(2) + ENTRY_LEN / 2,
			log.start(2) + 8,
			log.start(2) + ENTRY_LEN - 1,
			log.start(2),
		] {
			log.rewrite(&bytes[..cut]);
			assert_eq!(log.check_all(), [true, true, false, false], "{level:?}, cut at {cut}");
		}
		log.rewrite(&bytes);
		assert_eq!(log.check_all(), [true; 4], "the file is whole again");
	}
}

#[test]
fn a_pointer_past_the_end_of_the_file_is_not_in_the_log_and_is_not_a_panic() {
	for level in LEVELS {
		let log = Plain::new(level, 2);
		for offset in [log.bytes().len() as u64, 1 << 40, u64::MAX - 5, u64::MAX] {
			let past = ValuePointer {
				offset,
				..log.pointers[0].clone()
			};
			assert!(!check(&log.vlog, &past).unwrap(), "{level:?}, offset {offset}");
		}
		let huge = ValuePointer {
			key_size: u32::MAX,
			value_size: u32::MAX,
			..log.pointers[0].clone()
		};
		assert!(!check(&log.vlog, &huge).unwrap(), "{level:?}: lengths that no file holds");
	}
}

#[test]
fn an_entry_whose_header_says_other_lengths_is_not_in_the_log() {
	for level in LEVELS {
		for at in [0, 3, 4, 7] {
			let log = Plain::new(level, 3);
			let mut bytes = log.bytes();
			bytes[log.start(1) + at] ^= 0x01;
			log.rewrite(&bytes);
			assert_eq!(log.check_all(), [true, false, true], "{level:?}, header byte {at}");
		}
	}
}

#[test]
fn an_entry_whose_stored_checksum_is_not_the_one_of_its_pointer_is_not_in_the_log() {
	for level in LEVELS {
		for back in 1..=4 {
			let log = Plain::new(level, 3);
			let mut bytes = log.bytes();
			bytes[log.start(1) + ENTRY_LEN - back] ^= 0x80;
			log.rewrite(&bytes);
			assert_eq!(log.check_all(), [true, false, true], "{level:?}, checksum byte {back}");
		}
		// And the pointer is the one that says another checksum.
		let log = Plain::new(level, 2);
		let wrong = ValuePointer {
			checksum: log.pointers[0].checksum ^ 1,
			..log.pointers[0].clone()
		};
		assert!(!check(&log.vlog, &wrong).unwrap(), "{level:?}");
	}
}

/// A page that is missing in the middle of a value leaves the header and the checksum that was
/// stored with it: only the checksum of the key and the value tells. A read that does not verify
/// returns what is in the file, and the check does not depend on what the log reads with.
#[test]
fn a_page_that_is_missing_in_the_middle_of_a_value_is_told_whatever_the_log_verifies() {
	for level in LEVELS {
		let log = Plain::new(level, 3);
		let mut bytes = log.bytes();
		let middle = log.start(1) + 8 + KEY_LEN + VALUE_LEN / 2;
		bytes[middle..middle + 20].fill(0);
		log.rewrite(&bytes);

		assert_eq!(log.check_all(), [true, false, true], "{level:?}");
		let read = log.vlog.get(&log.pointers[1]);
		match level {
			VLogChecksumLevel::Disabled => {
				assert_ne!(read.unwrap(), value_of(1, VALUE_LEN), "the read returns what is there");
			}
			VLogChecksumLevel::Full => assert!(matches!(read, Err(Error::Corruption(_)))),
		}
	}
}

#[test]
fn a_key_that_changed_is_not_in_the_log() {
	let log = Plain::new(VLogChecksumLevel::Disabled, 2);
	let mut bytes = log.bytes();
	bytes[log.start(0) + 8] ^= 0x20;
	log.rewrite(&bytes);
	assert_eq!(log.check_all(), [false, true]);
}

#[test]
fn a_pointer_to_a_sealed_entry_in_a_file_that_is_not_sealed_is_an_error_and_not_a_missing_value() {
	let log = Plain::new(VLogChecksumLevel::Full, 1);
	let sealed = ValuePointer::sealed(
		1,
		log.pointers[0].offset,
		log.pointers[0].key_size,
		log.pointers[0].value_size,
		log.pointers[0].checksum,
	);
	assert!(matches!(check(&log.vlog, &sealed), Err(Error::Corruption(_))));
}

/// A read that fails is an error of the check: a value that cannot be looked for is not one that is
/// known to be missing.
#[cfg(unix)]
#[test]
fn a_file_that_cannot_be_read_is_an_error_and_not_a_missing_value() {
	let log = Plain::new(VLogChecksumLevel::Full, 1);
	// A directory where the file 7 would be: it is there, and it holds no header.
	std::fs::create_dir(log.opts.vlog_file_path(7)).unwrap();
	let unreadable = ValuePointer {
		file_id: 7,
		..log.pointers[0].clone()
	};
	assert!(check(&log.vlog, &unreadable).is_err());
}

#[test]
fn files_are_synced_through_the_log_and_only_those_that_are_named() {
	let dir = TempDir::new().unwrap();
	let mut opts = (*options(dir.path(), VLogChecksumLevel::Full, None)).clone();
	opts.vlog_max_file_size = 150;
	let opts = Arc::new(opts);
	let files: BTreeSet<u32> = {
		let vlog = VLog::new(Arc::clone(&opts)).unwrap();
		let files =
			(0..9).map(|i| vlog.append(&key_of(i), &value_of(i, 40)).unwrap().file_id).collect();
		vlog.flush().unwrap();
		files
	};
	assert_eq!(files.len(), 5, "the log rolled over: {files:?}");

	// A log that is opened on files that an earlier run left holds its newest file as unsynced,
	// and no other.
	let vlog = VLog::new(Arc::clone(&opts)).unwrap();
	let synced = || -> Vec<PathBuf> {
		crate::vlog::SYNCED_VLOG_FILES
			.lock()
			.iter()
			.filter(|path| path.starts_with(opts.vlog_dir()))
			.cloned()
			.collect()
	};
	assert!(synced().is_empty());
	vlog.sync_files(&[2].into_iter().collect()).unwrap();
	assert_eq!(
		synced(),
		[opts.vlog_file_path(2), opts.vlog_file_path(5)],
		"the file that is named, and the one that is appended to, as every sync does it"
	);

	let missing: BTreeSet<u32> = [99].into_iter().collect();
	assert!(vlog.sync_files(&missing).is_err(), "a file that is not there is an error");
	assert!(vlog.sync_files(&BTreeSet::new()).is_ok());
	assert_eq!(synced().len(), 2, "no file is synced for none");
}

// ---------------------------------------------------------------------------------------------
// Sealed entries
// ---------------------------------------------------------------------------------------------

fn context(suite: CipherSuite) -> Arc<EncryptionContext> {
	Arc::new(EncryptionContext::new(
		Arc::new(SoftwareKeyManager::new([0x11; 32])),
		DbId::from_bytes([0xD0; 16]),
		suite,
	))
}

/// A sealed file made by hand, as the tests of the sealed log make it: its header and the
/// entries, each sealed at its own offset.
fn sealed_file(
	opts: &Options,
	ctx: &EncryptionContext,
	entries: &[(&[u8], &[u8])],
) -> Vec<ValuePointer> {
	let path = opts.vlog_file_path(1);
	let (mut file, cipher) = sealed::create(&path, 1, opts.vlog_max_file_size, 0, ctx).unwrap();
	let mut offset = SEALED_HEADER_LEN as u64;
	let mut pointers = Vec::new();
	for (key, value) in entries {
		let mut stored = sealed::frame(key, value).unwrap();
		sealed::seal(&cipher, offset, &mut stored).unwrap();
		file.write_all(&stored).unwrap();
		pointers.push(ValuePointer::sealed(
			1,
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

struct Sealed {
	_dir: TempDir,
	opts: Arc<Options>,
	vlog: VLog,
	pointers: Vec<ValuePointer>,
	file: PathBuf,
}

impl Sealed {
	fn new(suite: CipherSuite, level: VLogChecksumLevel) -> Self {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let opts = options(dir.path(), level, Some(&ctx));
		let pointers = sealed_file(
			&opts,
			&ctx,
			&[(b"key-0", b"value-0"), (b"key-1", &[7u8; 200]), (b"key-2", b"value-2")],
		);
		let vlog = VLog::new(Arc::clone(&opts)).unwrap();
		let file = opts.vlog_file_path(1);
		Self {
			_dir: dir,
			opts,
			vlog,
			pointers,
			file,
		}
	}

	fn check_all(&self) -> Vec<bool> {
		self.pointers.iter().map(|pointer| check(&self.vlog, pointer).unwrap()).collect()
	}
}

#[test]
fn a_sealed_entry_that_is_whole_is_in_the_log() {
	for suite in SUITES {
		for level in LEVELS {
			let log = Sealed::new(suite, level);
			assert_eq!(log.check_all(), [true; 3], "{suite:?}, {level:?}");
			let cache_id = log.vlog.cache_id(1);
			assert!(
				log.opts.block_cache.get_vlog(cache_id, log.pointers[1].offset).is_none(),
				"nothing is cached"
			);
		}
	}
}

#[test]
fn a_sealed_entry_that_the_end_of_its_file_cuts_short_is_not_in_the_log() {
	for suite in SUITES {
		for level in LEVELS {
			let log = Sealed::new(suite, level);
			let bytes = std::fs::read(&log.file).unwrap();
			let second = log.pointers[1].offset as usize;
			let len = log.pointers[1].total_entry_size() as usize;
			for cut in [second + len / 2, second + len - 1, second] {
				std::fs::write(&log.file, &bytes[..cut]).unwrap();
				assert_eq!(
					log.check_all(),
					[true, false, false],
					"{suite:?}, {level:?}, cut at {cut}"
				);
			}
		}
	}
}

#[test]
fn a_sealed_entry_that_changed_is_not_in_the_log() {
	for suite in SUITES {
		for level in LEVELS {
			let log = Sealed::new(suite, level);
			let mut bytes = std::fs::read(&log.file).unwrap();
			let second = log.pointers[1].offset as usize;
			let len = log.pointers[1].total_entry_size() as usize;
			// A byte of the ciphertext, a byte of the tag, and a page that is zeros.
			bytes[second + 9] ^= 0x01;
			std::fs::write(&log.file, &bytes).unwrap();
			assert_eq!(log.check_all(), [true, false, true], "{suite:?}, {level:?}");
			bytes[second + 9] ^= 0x01;
			bytes[second + len - 1] ^= 0x01;
			std::fs::write(&log.file, &bytes).unwrap();
			assert_eq!(log.check_all(), [true, false, true], "{suite:?}, {level:?}");
			bytes[second + len - 1] ^= 0x01;
			bytes[second + 20..second + 60].fill(0);
			std::fs::write(&log.file, &bytes).unwrap();
			assert_eq!(log.check_all(), [true, false, true], "{suite:?}, {level:?}");
		}
	}
}

/// What can be told without the key is the checksum of what is stored. An entry that was changed,
/// and a pointer that was made to fit it, are not told by the check: the read, which opens the
/// entry with the key, is what refuses it.
#[test]
fn what_a_check_cannot_tell_of_a_sealed_entry_without_the_key_is_left_to_the_read() {
	let log = Sealed::new(CipherSuite::Aes256Gcm, VLogChecksumLevel::Disabled);
	let mut bytes = std::fs::read(&log.file).unwrap();
	let second = log.pointers[1].offset as usize;
	bytes[second + 9] ^= 0x01;
	std::fs::write(&log.file, &bytes).unwrap();
	let len = log.pointers[1].total_entry_size() as usize;
	let fitted = ValuePointer {
		checksum: crc32fast::hash(&bytes[second..second + len]),
		..log.pointers[1].clone()
	};

	assert!(check(&log.vlog, &fitted).unwrap(), "the checksum of what is stored fits");
	match log.vlog.get(&fitted) {
		Err(Error::Encryption(error)) => assert_eq!(error, EncryptionError::AuthenticationFailed),
		other => panic!("the read does not open the entry: {other:?}"),
	}
}

#[test]
fn a_pointer_to_a_plaintext_entry_in_a_sealed_file_is_an_error_and_not_a_missing_value() {
	let log = Sealed::new(CipherSuite::Aes256Gcm, VLogChecksumLevel::Full);
	let plain = ValuePointer::new(
		1,
		log.pointers[0].offset,
		log.pointers[0].key_size,
		log.pointers[0].value_size,
		log.pointers[0].checksum,
	);
	assert!(matches!(check(&log.vlog, &plain), Err(Error::Corruption(_))));
}

#[test]
fn a_sealed_file_that_is_not_there_holds_no_entry() {
	let log = Sealed::new(CipherSuite::Aes256Gcm, VLogChecksumLevel::Full);
	let elsewhere = ValuePointer {
		file_id: 5,
		..log.pointers[0].clone()
	};
	assert!(!check(&log.vlog, &elsewhere).unwrap());
}
