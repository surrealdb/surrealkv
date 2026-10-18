//! The manifest of an encrypted database: sealed on every write under a salt of its own, opened
//! with the key of the database, never read as the other kind, and never changed by a read that
//! was refused.
//!
//! These are about the file and the loader. How a database decides to be encrypted, and what a
//! tree does with a manifest that is not its own, is in `encryption_open_tests`.

use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tempfile::TempDir;

use super::tfs;
use crate::error::EncryptionError;
use crate::levels::{
	check_manifest_before_open,
	fail_replacements,
	manifest_kind,
	unseal_manifest,
	write_manifest_to_disk,
	LevelManifest,
	ManifestKind,
	ReplaceFailure,
	SnapshotInfo,
	MANIFEST_FORMAT_VERSION,
};
use crate::tde::{
	is_sealed_manifest,
	open_manifest,
	CipherSuite,
	CryptoHeader,
	DbId,
	EncryptionContext,
	SoftwareKeyManager,
	HEADER_LEN,
	MANIFEST_MAGIC,
	MANIFEST_PREFIX_LEN,
};
use crate::{Error, Options};

const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

const ROOT: [u8; 32] = [0x42; 32];

/// Where the crypto header starts in the file.
const HEADER_AT: usize = MANIFEST_PREFIX_LEN - HEADER_LEN;

/// The context of the database `db` under the root `root`.
fn context(suite: CipherSuite, root: [u8; 32], db: u8) -> Arc<EncryptionContext> {
	Arc::new(EncryptionContext::new(
		Arc::new(SoftwareKeyManager::new(root)),
		DbId::from_bytes([db; 16]),
		suite,
	))
}

/// Options for a database of three levels in `dir`, with its manifest directory made.
fn options(dir: &Path, ctx: Option<&Arc<EncryptionContext>>) -> Arc<Options> {
	let opts = Options {
		path: dir.to_path_buf(),
		level_count: 3,
		..Default::default()
	};
	let opts = match ctx {
		Some(ctx) => opts.with_encryption_context(Arc::clone(ctx)),
		None => opts,
	};
	tfs::create_dir_all(&opts, opts.manifest_dir()).unwrap();
	Arc::new(opts)
}

/// A manifest that holds something of each kind that a manifest without tables can hold.
fn manifest_with_content(opts: &Arc<Options>) -> LevelManifest {
	let mut manifest = LevelManifest::new(Arc::clone(opts)).unwrap();
	manifest.next_table_id.store(42, Ordering::SeqCst);
	manifest.last_sequence = 17;
	manifest.snapshots.push(SnapshotInfo {
		seq_num: 5,
		created_at: 99,
	});
	write_manifest_to_disk(&manifest).unwrap();
	manifest
}

fn salt_of(file: &[u8]) -> [u8; 32] {
	CryptoHeader::decode(&file[HEADER_AT..]).unwrap().salt
}

/// The error of an operation that is expected to fail with an encryption error.
fn encryption_error<T>(result: crate::Result<T>, what: &str) -> EncryptionError {
	match result {
		Err(Error::Encryption(err)) => err,
		Err(other) => panic!("{what}: expected an encryption error, got {other:?}"),
		Ok(_) => panic!("{what}: it succeeded"),
	}
}

fn is_corruption<T>(result: &crate::Result<T>) -> bool {
	matches!(result, Err(Error::ManifestCorruption(_)))
}

/// The header's checksum made again after a byte of the header was changed.
fn with_fixed_header_crc(mut bytes: Vec<u8>) -> Vec<u8> {
	let crc_at = MANIFEST_PREFIX_LEN - 4;
	let crc = crc32fast::hash(&bytes[HEADER_AT..crc_at]);
	bytes[crc_at..MANIFEST_PREFIX_LEN].copy_from_slice(&crc.to_be_bytes());
	bytes
}

/// What the frozen loader reads of a manifest.
#[derive(Debug)]
pub(super) struct OldManifest {
	next_table_id: u64,
	last_sequence: u64,
	levels: Vec<Vec<u64>>,
	snapshots: usize,
}

/// How a manifest was read before manifests could be encrypted, written out here from the format
/// and not from the code that reads it now: the version first, which must be 1, then the three
/// counters, the tables of each level and the snapshots. It stands for a binary of that time,
/// which refuses the manifest of the current version (2) on its first two bytes: use
/// `as_version_1` for the layout.
pub(super) fn frozen_old_decode(data: &[u8]) -> Result<OldManifest, String> {
	let mut reader = std::io::Cursor::new(data);
	let mut read = |len: usize| -> Result<Vec<u8>, String> {
		let mut bytes = vec![0u8; len];
		reader.read_exact(&mut bytes).map_err(|e| e.to_string())?;
		Ok(bytes)
	};
	let version = u16::from_be_bytes(read(2)?.try_into().unwrap());
	if version != 1 {
		return Err(format!("Unsupported manifest format version: {version}. Expected: 1"));
	}
	let next_table_id = u64::from_be_bytes(read(8)?.try_into().unwrap());
	let log_number = u64::from_be_bytes(read(8)?.try_into().unwrap());
	let last_sequence = u64::from_be_bytes(read(8)?.try_into().unwrap());
	let level_count = read(1)?[0];
	let mut levels = Vec::new();
	for _ in 0..level_count {
		let tables = u32::from_be_bytes(read(4)?.try_into().unwrap());
		let mut ids = Vec::new();
		for _ in 0..tables {
			ids.push(u64::from_be_bytes(read(8)?.try_into().unwrap()));
		}
		levels.push(ids);
	}
	let snapshots = u32::from_be_bytes(read(4)?.try_into().unwrap());
	for _ in 0..snapshots {
		let len = u32::from_be_bytes(read(4)?.try_into().unwrap()) as usize;
		read(len)?;
	}
	let _ = log_number;
	Ok(OldManifest {
		next_table_id,
		last_sequence,
		levels,
		snapshots: snapshots as usize,
	})
}

/// The manifest `plain`, of the current version, with the version of the format that SurrealKV up
/// to v0.21.4 wrote: the layout of the two is the same, and only the number differs.
fn as_version_1(plain: &[u8]) -> Vec<u8> {
	let mut bytes = plain.to_vec();
	bytes[..2].copy_from_slice(&1u16.to_be_bytes());
	bytes
}

#[test]
fn the_manifest_of_an_encrypted_database_is_sealed_and_its_body_is_what_a_plaintext_one_holds() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite, ROOT, 0xD1);
		let opts = options(dir.path(), Some(&ctx));
		let path = opts.manifest_file_path(0);
		let mut manifest = manifest_with_content(&opts);
		let sealed = fs::read(&path).unwrap();
		assert!(is_sealed_manifest(&sealed), "{suite:?}");
		assert_eq!(&sealed[..MANIFEST_MAGIC.len()], &MANIFEST_MAGIC);
		assert_eq!(manifest_kind(&sealed), ManifestKind::Sealed);

		// The same manifest written without a context is the encoding of a plaintext database,
		// and what the sealed one holds is that, byte for byte.
		manifest.encryption = None;
		write_manifest_to_disk(&manifest).unwrap();
		let plain = fs::read(&path).unwrap();
		assert_eq!(plain[..2], MANIFEST_FORMAT_VERSION.to_be_bytes(), "{suite:?}");
		assert_eq!(open_manifest(&ctx, &sealed).unwrap(), plain, "{suite:?}");
		assert_eq!(sealed.len(), plain.len() + MANIFEST_PREFIX_LEN + 16, "{suite:?}");

		// Written sealed again, it is loaded with the key, and says what was written.
		manifest.encryption = Some(Arc::clone(&ctx));
		write_manifest_to_disk(&manifest).unwrap();
		let loaded = LevelManifest::load_from_file(&path, Arc::clone(&opts)).unwrap();
		assert_eq!(loaded.next_table_id.load(Ordering::SeqCst), 42);
		assert_eq!(loaded.last_sequence, 17);
		assert_eq!(loaded.snapshots.len(), 1);
		assert_eq!((loaded.snapshots[0].seq_num, loaded.snapshots[0].created_at), (5, 99));
		assert_eq!(loaded.levels.get_levels().len(), 3);
		assert_eq!(loaded.encryption.as_ref().map(|c| c.db_id()), Some(ctx.db_id()));
		assert_eq!(loaded.encryption.as_ref().map(|c| c.suite()), Some(suite));

		// And `new` finds and loads it as it does a plaintext one.
		let reopened = LevelManifest::new(Arc::clone(&opts)).unwrap();
		assert_eq!(reopened.last_sequence, 17);
	}
}

#[test]
fn a_manifest_without_a_context_is_the_bytes_that_a_plaintext_database_always_had() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path(), None);
	LevelManifest::new(Arc::clone(&opts)).unwrap();

	// The current version, next table id 1, log number 0, last sequence 0, three levels without
	// tables, and no snapshots: written out here, not taken from the code that writes it.
	let mut expected = MANIFEST_FORMAT_VERSION.to_be_bytes().to_vec();
	expected.extend_from_slice(&1u64.to_be_bytes());
	expected.extend_from_slice(&0u64.to_be_bytes());
	expected.extend_from_slice(&0u64.to_be_bytes());
	expected.push(3);
	expected.extend_from_slice(&[0u8; 12]);
	expected.extend_from_slice(&[0u8; 4]);

	// Then the checksum trailer: the length of the body, its masked CRC-32, and the magic.
	let mut written = expected.clone();
	written.extend_from_slice(&(expected.len() as u32).to_be_bytes());
	written.extend_from_slice(&crate::levels::calculate_manifest_crc(&expected).to_be_bytes());
	written.extend_from_slice(b"SKVMNFST");
	assert_eq!(fs::read(opts.manifest_file_path(0)).unwrap(), written);
	// The layout is the one that was always written; only the version is not the one that a binary
	// of that time knows, and it refuses it.
	assert!(frozen_old_decode(&as_version_1(&expected)).is_ok());
	assert!(frozen_old_decode(&expected).unwrap_err().contains("Unsupported manifest format"));
}

#[test]
fn every_write_of_the_manifest_is_sealed_under_a_new_salt() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite, ROOT, 0xD1);
		let opts = options(dir.path(), Some(&ctx));
		let path = opts.manifest_file_path(0);
		let manifest = manifest_with_content(&opts);

		let mut files = vec![fs::read(&path).unwrap()];
		for _ in 0..3 {
			write_manifest_to_disk(&manifest).unwrap();
			files.push(fs::read(&path).unwrap());
		}
		// The same content every time, and a different file: salt, tag and ciphertext.
		let salts: BTreeSet<_> = files.iter().map(|file| salt_of(file)).collect();
		let bodies: BTreeSet<_> =
			files.iter().map(|file| file[MANIFEST_PREFIX_LEN..].to_vec()).collect();
		assert_eq!(salts.len(), 4, "{suite:?}");
		assert_eq!(bodies.len(), 4, "{suite:?}");
		assert_eq!(files.iter().map(Vec::len).collect::<BTreeSet<_>>().len(), 1, "{suite:?}");
		let first = open_manifest(&ctx, &files[0]).unwrap();
		for file in &files {
			assert_eq!(open_manifest(&ctx, file).unwrap(), first, "{suite:?}");
		}
	}
}

#[test]
fn a_write_that_is_tried_again_after_a_failure_is_sealed_under_a_new_salt() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite, ROOT, 0xD1);
		let opts = options(dir.path(), Some(&ctx));
		let path = opts.manifest_file_path(0);
		let manifest = manifest_with_content(&opts);
		let first = fs::read(&path).unwrap();

		// A failure before the rename leaves the file as it was, and the write that follows it
		// has a salt of its own.
		fail_replacements(&path, ReplaceFailure::BeforeRename, 0, 1);
		let err = write_manifest_to_disk(&manifest).unwrap_err();
		assert!(matches!(err, Error::Io(_)), "{suite:?}: {err:?}");
		assert_eq!(fs::read(&path).unwrap(), first, "{suite:?}: the failed write changed the file");
		write_manifest_to_disk(&manifest).unwrap();
		let second = fs::read(&path).unwrap();
		assert_ne!(salt_of(&second), salt_of(&first), "{suite:?}");

		// A failure after the rename leaves the new file in place, which opens with the key and
		// has a salt of its own, and the write that follows it has another.
		fail_replacements(&path, ReplaceFailure::AfterRename, 0, 1);
		let err = write_manifest_to_disk(&manifest).unwrap_err();
		assert!(matches!(err, Error::ManifestWriteUncertain(_)), "{suite:?}: {err:?}");
		let third = fs::read(&path).unwrap();
		assert_ne!(third, second, "{suite:?}: the write that failed after the rename is in place");
		assert_ne!(salt_of(&third), salt_of(&second), "{suite:?}");
		assert!(open_manifest(&ctx, &third).is_ok(), "{suite:?}");
		write_manifest_to_disk(&manifest).unwrap();
		let fourth = fs::read(&path).unwrap();
		let salts: BTreeSet<_> = [&first, &second, &third, &fourth].map(|f| salt_of(f)).into();
		assert_eq!(salts.len(), 4, "{suite:?}: a salt was used twice");
	}
}

#[test]
fn a_manifest_is_not_read_as_the_other_kind_and_nothing_is_changed_by_the_refusal() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite, ROOT, 0xD1);
		let opts = options(dir.path(), Some(&ctx));
		let path = opts.manifest_file_path(0);
		manifest_with_content(&opts);
		let before = fs::read(&path).unwrap();

		let load = |opts: &Arc<Options>| LevelManifest::load_from_file(&path, Arc::clone(opts));
		let check = |opts: &Arc<Options>| check_manifest_before_open(opts);

		// An encrypted manifest and no key.
		let none = options(dir.path(), None);
		assert_eq!(encryption_error(load(&none), "load"), EncryptionError::NotConfigured);
		assert_eq!(encryption_error(check(&none), "check"), EncryptionError::NotConfigured);

		// The same database under another root, and the same root in another database.
		let other_root = options(dir.path(), Some(&context(suite, [0x43; 32], 0xD1)));
		assert_eq!(encryption_error(load(&other_root), "load"), EncryptionError::WrongKey);
		assert_eq!(encryption_error(check(&other_root), "check"), EncryptionError::WrongKey);
		let other_db = options(dir.path(), Some(&context(suite, ROOT, 0xD2)));
		assert_eq!(encryption_error(load(&other_db), "load"), EncryptionError::DbIdMismatch);
		assert_eq!(encryption_error(check(&other_db), "check"), EncryptionError::DbIdMismatch);

		// The right key.
		assert!(load(&opts).is_ok());
		assert!(check(&opts).is_ok());
		assert_eq!(fs::read(&path).unwrap(), before, "{suite:?}: a read changed the manifest");

		// A manifest that is not encrypted, where there is a key.
		let plain_dir = TempDir::new().unwrap();
		let plain = options(plain_dir.path(), None);
		manifest_with_content(&plain);
		let plain_path = plain.manifest_file_path(0);
		let plain_before = fs::read(&plain_path).unwrap();
		let keyed = options(plain_dir.path(), Some(&ctx));
		let err = LevelManifest::load_from_file(&plain_path, Arc::clone(&keyed));
		assert_eq!(encryption_error(err, "load"), EncryptionError::NotEncrypted);
		assert_eq!(encryption_error(check(&keyed), "check"), EncryptionError::NotEncrypted);
		assert!(check(&plain).is_ok());
		assert_eq!(fs::read(&plain_path).unwrap(), plain_before);
	}
}

#[test]
fn a_loader_that_does_not_know_encryption_fails_at_the_manifest_with_nothing_else_read() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let opts = options(dir.path(), Some(&context(suite, ROOT, 0xD1)));
		let path = opts.manifest_file_path(0);
		let mut manifest = manifest_with_content(&opts);
		let sealed = fs::read(&path).unwrap();
		manifest.encryption = None;
		write_manifest_to_disk(&manifest).unwrap();
		let plain = fs::read(&path).unwrap();

		// The frozen loader reads what a plaintext database wrote, which is what the sealed one
		// holds, and it is the first thing a binary of that time reads of a database.
		let old = frozen_old_decode(&as_version_1(&plain)).unwrap();
		assert_eq!(
			(old.next_table_id, old.last_sequence, old.levels.len(), old.snapshots),
			(42, 17, 3, 1)
		);

		// An encrypted manifest does not begin with the version that it reads first.
		assert_ne!(sealed[..2], MANIFEST_FORMAT_VERSION.to_be_bytes(), "{suite:?}");
		let err = frozen_old_decode(&sealed).unwrap_err();
		assert!(err.contains("Unsupported manifest format version"), "{suite:?}: {err}");
	}
}

#[test]
fn a_changed_manifest_is_the_error_of_the_part_that_changed_and_a_read_changes_nothing() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite, ROOT, 0xD1);
		let opts = options(dir.path(), Some(&ctx));
		let path = opts.manifest_file_path(0);
		manifest_with_content(&opts);
		let sealed = fs::read(&path).unwrap();

		let flip = |at: usize| {
			let mut bytes = sealed.clone();
			bytes[at] ^= 0x01;
			bytes
		};
		let last = sealed.len() - 1;
		enum Want {
			Damage,
			Key(EncryptionError),
		}
		let cases: Vec<(&str, Vec<u8>, Want)> = vec![
			// A byte of the magic: the file begins like nothing that is known.
			("magic", flip(0), Want::Damage),
			("version", flip(MANIFEST_MAGIC.len()), Want::Key(EncryptionError::UnsupportedVersion)),
			("length", flip(MANIFEST_MAGIC.len() + 1 + 7), Want::Damage),
			("header magic", flip(HEADER_AT), Want::Key(EncryptionError::MalformedHeader)),
			(
				"header checksum",
				flip(MANIFEST_PREFIX_LEN - 1),
				Want::Key(EncryptionError::MalformedHeader),
			),
			(
				"database id, checksum made again",
				with_fixed_header_crc(flip(HEADER_AT + 16)),
				Want::Key(EncryptionError::DbIdMismatch),
			),
			(
				"salt, checksum made again",
				with_fixed_header_crc(flip(HEADER_AT + 40)),
				Want::Key(EncryptionError::WrongKey),
			),
			(
				"first byte of the body",
				flip(MANIFEST_PREFIX_LEN),
				Want::Key(EncryptionError::AuthenticationFailed),
			),
			("last byte of the tag", flip(last), Want::Key(EncryptionError::AuthenticationFailed)),
		];
		for (name, bytes, want) in cases {
			fs::write(&path, &bytes).unwrap();
			let loaded = LevelManifest::load_from_file(&path, Arc::clone(&opts));
			let checked = check_manifest_before_open(&opts);
			let what = format!("{suite:?}: {name}");
			match want {
				Want::Damage => {
					assert!(is_corruption(&loaded), "{what}");
					assert!(is_corruption(&checked), "{what}");
				}
				Want::Key(expected) => {
					assert_eq!(encryption_error(loaded, &what), expected, "{what}");
					assert_eq!(encryption_error(checked, &what), expected, "{what}");
				}
			}
			assert_eq!(fs::read(&path).unwrap(), bytes, "{what}: a read changed the manifest");
		}

		// The file as it was is the database again.
		fs::write(&path, &sealed).unwrap();
		assert!(LevelManifest::load_from_file(&path, Arc::clone(&opts)).is_ok());
	}
}

#[test]
fn a_manifest_that_is_cut_short_is_damage_at_every_length_and_never_a_panic() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let opts = options(dir.path(), Some(&context(suite, ROOT, 0xD1)));
		let path = opts.manifest_file_path(0);
		manifest_with_content(&opts);
		let sealed = fs::read(&path).unwrap();

		// Every length up to the end of the prefix and a few in the body: before the magic is
		// whole, inside the version, the length and the header, at the end of the prefix, with no
		// tag and with the tag cut.
		let mut lengths: Vec<usize> = (0..=MANIFEST_PREFIX_LEN + 1).collect();
		lengths.extend([
			MANIFEST_PREFIX_LEN + 20,
			sealed.len() - 17,
			sealed.len() - 16,
			sealed.len() - 1,
		]);
		for len in lengths {
			let what = format!("{suite:?}: {len} bytes");
			fs::write(&path, &sealed[..len]).unwrap();
			let loaded = LevelManifest::load_from_file(&path, Arc::clone(&opts));
			assert!(is_corruption(&loaded), "{what}: {:?}", loaded.err());
			let checked = check_manifest_before_open(&opts);
			assert!(is_corruption(&checked), "{what}: {:?}", checked.err());
			assert_eq!(fs::read(&path).unwrap(), sealed[..len], "{what}");
		}

		// Something after the tag is damage too.
		let mut longer = sealed.clone();
		longer.push(0);
		fs::write(&path, &longer).unwrap();
		assert!(is_corruption(&LevelManifest::load_from_file(&path, Arc::clone(&opts))));
	}
}

#[test]
fn the_first_bytes_of_a_manifest_say_what_kind_it_is() {
	assert_eq!(manifest_kind(&sealed_start()), ManifestKind::Sealed);
	assert_eq!(manifest_kind(b"SKVMENC"), ManifestKind::Sealed);
	assert_eq!(manifest_kind(b""), ManifestKind::CutShort);
	assert_eq!(manifest_kind(b"SK"), ManifestKind::CutShort);
	assert_eq!(manifest_kind(b"SKVMEN"), ManifestKind::CutShort);
	// A plaintext manifest begins with a small version that is not zero.
	assert_eq!(manifest_kind(&[0, 1, 9, 9]), ManifestKind::Plain);
	assert_eq!(manifest_kind(&[0, 2]), ManifestKind::Plain);
	assert_eq!(manifest_kind(&[0, 255]), ManifestKind::Plain);
	assert_eq!(manifest_kind(&[0, 0, 0, 0]), ManifestKind::Unknown);
	assert_eq!(manifest_kind(&[1, 0]), ManifestKind::Unknown);
	assert_eq!(manifest_kind(&[0]), ManifestKind::Unknown);
	assert_eq!(manifest_kind(b"RKVMENC and more"), ManifestKind::Unknown);
	assert_eq!(manifest_kind(&[0xFF; 100]), ManifestKind::Unknown);

	// Read without a key, a manifest that is not encrypted is given back as it is, whatever it
	// holds, for its decoder to judge.
	assert_eq!(unseal_manifest(vec![0, 1, 2], None).unwrap(), vec![0, 1, 2]);
	assert_eq!(unseal_manifest(vec![], None).unwrap(), Vec::<u8>::new());
	assert_eq!(
		encryption_error(unseal_manifest(sealed_start(), None), "no key"),
		EncryptionError::NotConfigured
	);
}

fn sealed_start() -> Vec<u8> {
	let mut bytes = MANIFEST_MAGIC.to_vec();
	bytes.push(1);
	bytes
}
