//! What a file says about its encryption without a key: every surface of a database that was
//! written by a real tree, the shapes that a write that did not finish leaves, and the headers
//! that are damaged or of another kind.
//!
//! The fields of a header are also read here from the layout that the format documents (the
//! offsets of the suite, the key id, the database, the file and the salt in the 92 bytes, and
//! where the 92 bytes begin in each kind of file), so that they are compared with what the code
//! says and not only with themselves.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use super::encryption_open_tests::{create, files_with_extension, manager, plain, SUITES};
use crate::error::EncryptionError;
use crate::inspect::{describe_file, is_encrypted, FileDescription, FileState};
use crate::sstable::table::TableWriter;
use crate::tde::{
	describe_manifest_file,
	seal_manifest,
	CipherSuite,
	DbId,
	EncryptionContext,
	FileKind,
	HEADER_LEN,
	MANIFEST_PREFIX_LEN,
};
use crate::{InternalKey, InternalKeyKind, Options, Tree};

/// Where the 92 byte header begins in a sealed file of each kind, from the format: after the 16
/// byte prefix of the manifest, at the start of a table, after the 31 byte file header and its
/// 4 byte checksum in a value-log file, and after the 7 byte framing of the first record of a
/// segment.
fn header_at(kind: FileKind) -> usize {
	match kind {
		FileKind::Manifest => 16,
		FileKind::Sst => 0,
		FileKind::Vlog => 35,
		FileKind::Wal => 7,
	}
}

/// The fields of the header at `at`, read at the offsets that the format gives: the suite at 5,
/// the key id at 8..12, the database at 16..32, the file at 32..40 and the salt at 40..72.
struct Fields {
	suite: u8,
	key_id: u32,
	db_id: [u8; 16],
	file_id: u64,
	salt: [u8; 32],
}

fn fields_at(bytes: &[u8], at: usize) -> Fields {
	let header = &bytes[at..at + HEADER_LEN];
	assert_eq!(&header[..4], b"SKTD", "not a header at {at}");
	Fields {
		suite: header[5],
		key_id: u32::from_be_bytes(header[8..12].try_into().unwrap()),
		db_id: header[16..32].try_into().unwrap(),
		file_id: u64::from_be_bytes(header[32..40].try_into().unwrap()),
		salt: header[40..72].try_into().unwrap(),
	}
}

/// The same header with its checksum computed again, which is the last four bytes and covers the
/// ones before them.
fn with_fixed_crc(mut header: [u8; HEADER_LEN]) -> [u8; HEADER_LEN] {
	let crc = crc32fast::hash(&header[..HEADER_LEN - 4]);
	header[HEADER_LEN - 4..].copy_from_slice(&crc.to_be_bytes());
	header
}

fn id_of(path: &Path) -> u64 {
	let name = path.file_name().unwrap().to_str().unwrap();
	name[..20].parse().unwrap()
}

/// The files of the four surfaces of the database in `root`: the manifest, the tables, the value
/// log and the write-ahead log, with what each is.
fn surfaces(root: &Path) -> Vec<(FileKind, PathBuf)> {
	let opts = Options {
		path: root.to_path_buf(),
		..Default::default()
	};
	let mut files = vec![(FileKind::Manifest, opts.manifest_file_path(0))];
	for (kind, dir, extension) in [
		(FileKind::Sst, opts.sstable_dir(), "sst"),
		(FileKind::Vlog, opts.vlog_dir(), "vlog"),
		(FileKind::Wal, opts.wal_dir(), "wal"),
	] {
		let found = files_with_extension(&dir, extension);
		assert!(!found.is_empty(), "the database has no {extension} file");
		files.extend(found.into_iter().map(|path| (kind, path)));
	}
	files
}

fn described(path: &Path) -> FileDescription {
	describe_file(path).unwrap()
}

fn sealed(description: &FileDescription) -> crate::inspect::CryptoSummary {
	match &description.state {
		FileState::Sealed(summary) => *summary,
		other => panic!("{:?} is {other:?}", description.path),
	}
}

#[tokio::test]
async fn every_file_of_an_encrypted_database_names_its_key_and_database_without_a_key() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let opts = create(dir.path(), manager(), suite).await;
		let db_id = describe_manifest_file(opts.manifest_file_path(0)).unwrap().db_id.unwrap();

		let mut salts = HashSet::new();
		let files = surfaces(dir.path());
		assert!(files.len() >= 4, "{suite:?}: {files:?}");
		for (kind, path) in &files {
			let description = described(path);
			assert_eq!(description.kind, Some(*kind), "{path:?}");
			assert_eq!(description.file_size, fs::metadata(path).unwrap().len(), "{path:?}");
			assert!(description.is_encrypted(), "{path:?}");
			let summary = sealed(&description);
			assert_eq!(description.crypto(), Some(&summary));

			assert_eq!(summary.suite, suite, "{path:?}");
			assert_eq!(summary.key_id, 1, "{path:?}");
			assert_eq!(summary.db_id, db_id, "{path:?}");
			let expected_id = if *kind == FileKind::Manifest {
				0
			} else {
				id_of(path)
			};
			assert_eq!(summary.file_id, expected_id, "{path:?}");
			assert!(salts.insert(summary.salt), "{path:?} shares its salt with another file");

			// And the same fields where the format puts them.
			let bytes = fs::read(path).unwrap();
			let raw = fields_at(&bytes, header_at(*kind));
			assert_eq!(raw.suite, suite.id(), "{path:?}");
			assert_eq!(raw.key_id, summary.key_id, "{path:?}");
			assert_eq!(&raw.db_id, db_id.as_bytes(), "{path:?}");
			assert_eq!(raw.file_id, summary.file_id, "{path:?}");
			assert_eq!(raw.salt, summary.salt, "{path:?}");
		}
	}
}

#[tokio::test]
async fn a_database_in_the_clear_is_described_as_plaintext_in_every_file() {
	let dir = TempDir::new().unwrap();
	let opts = plain(dir.path());
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	let mut txn = tree.begin().unwrap();
	for i in 0..20 {
		txn.set(format!("pk-{i:03}").as_bytes(), "p".repeat(80).as_bytes()).unwrap();
	}
	txn.commit().await.unwrap();
	tree.flush().unwrap();
	let mut txn = tree.begin().unwrap();
	txn.set(b"pk-late", "q".repeat(80).as_bytes()).unwrap();
	txn.commit().await.unwrap();
	tree.close().await.unwrap();

	let files = surfaces(dir.path());
	assert!(files.len() >= 3, "{files:?}");
	for (kind, path) in files {
		let description = described(&path);
		assert_eq!(description.kind, Some(kind), "{path:?}");
		assert_eq!(description.state, FileState::Plaintext, "{path:?}");
		assert!(!description.is_encrypted() && description.crypto().is_none(), "{path:?}");
	}
	assert!(!is_encrypted(dir.path()).unwrap());
}

#[tokio::test]
async fn whether_a_database_a_directory_or_a_file_is_encrypted_is_answered_from_its_start() {
	let dir = TempDir::new().unwrap();
	let sealed_root = dir.path().join("sealed");
	let opts = create(&sealed_root, manager(), CipherSuite::XChaCha20Poly1305).await;
	assert!(is_encrypted(&sealed_root).unwrap());
	// The log directory alone, where there is no manifest to look at, and one file of each kind.
	assert!(is_encrypted(opts.wal_dir()).unwrap());
	assert!(is_encrypted(opts.sstable_dir()).unwrap());
	for (kind, path) in surfaces(&sealed_root) {
		assert!(is_encrypted(&path).unwrap(), "{kind:?}");
	}

	// Nothing there is nothing to say that it is encrypted.
	let empty = dir.path().join("empty");
	fs::create_dir_all(&empty).unwrap();
	assert!(!is_encrypted(&empty).unwrap());
	// A file that cannot be read at all is an error and not an answer.
	assert!(is_encrypted(dir.path().join("none.sst")).is_err());
	assert!(describe_file(dir.path().join("none.sst")).is_err());
}

/// A sealed table, and a sealed manifest, from the same database.
fn sealed_table_and_manifest(suite: CipherSuite) -> (Vec<u8>, Vec<u8>, DbId) {
	let db_id = DbId::from_bytes([0xD7; 16]);
	let context = Arc::new(EncryptionContext::new(manager(), db_id, suite));
	let opts = Arc::new(Options::default().with_encryption_context(Arc::clone(&context)));
	let mut table = Vec::new();
	{
		let mut writer = TableWriter::new(&mut table, 9, opts, 0);
		for i in 0..10u64 {
			let key = InternalKey::new(format!("key{i}").into_bytes(), i + 1, InternalKeyKind::Set);
			writer.add(key, b"a value").unwrap();
		}
		writer.finish().unwrap();
	}
	let manifest = seal_manifest(&context, b"the body of a manifest").unwrap();
	(table, manifest, db_id)
}

#[test]
fn a_file_that_a_write_did_not_finish_is_unfinished_and_never_an_error() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let (table, manifest, db_id) = sealed_table_and_manifest(suite);
		assert!(table.len() > 300, "the cuts below are inside the table: {}", table.len());
		let put = |name: &str, bytes: &[u8]| {
			let path = dir.path().join(name);
			fs::write(&path, bytes).unwrap();
			path
		};

		// Nothing, and zeros, in a file of each name.
		for name in ["a.sst", "a.wal", "a.vlog", "a.manifest"] {
			let kind = match name {
				"a.sst" => FileKind::Sst,
				"a.wal" => FileKind::Wal,
				"a.vlog" => FileKind::Vlog,
				_ => FileKind::Manifest,
			};
			for bytes in [Vec::new(), vec![0u8; 300]] {
				let description = described(&put(name, &bytes));
				assert_eq!(description.state, FileState::Unfinished, "{name} {}", bytes.len());
				assert_eq!(description.kind, Some(kind), "{name}");
				assert!(!description.is_encrypted());
			}
		}

		// A manifest that ends inside its header is a manifest that was not finished, and one that
		// holds the whole of it is described.
		let cut = described(&put("cut.manifest", &manifest[..MANIFEST_PREFIX_LEN - 1]));
		assert_eq!((cut.kind, cut.state), (Some(FileKind::Manifest), FileState::Unfinished));
		let whole = described(&put("whole.manifest", &manifest));
		assert_eq!(sealed(&whole).db_id, db_id);

		// A table that was cut off before its footer is told by its header, which is where its
		// blocks were sealed from, and one that ends inside its header is not yet anything.
		let no_footer = described(&put("00000000000000000009.sst", &table[..200]));
		assert_eq!(no_footer.kind, Some(FileKind::Sst));
		let summary = sealed(&no_footer);
		assert_eq!((summary.file_id, summary.db_id, summary.suite), (9, db_id, suite));
		let in_header = described(&put("00000000000000000010.sst", &table[..60]));
		assert_eq!(in_header.state, FileState::Unfinished);
	}
}

#[test]
fn a_header_that_is_damaged_or_of_another_kind_or_version_is_said_to_be() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let (table, _, _) = sealed_table_and_manifest(suite);
		let state_of = |bytes: &[u8]| {
			let path = dir.path().join("00000000000000000009.sst");
			fs::write(&path, bytes).unwrap();
			described(&path).state
		};
		assert!(matches!(state_of(&table), FileState::Sealed(_)));

		let header: [u8; HEADER_LEN] = table[..HEADER_LEN].try_into().unwrap();
		let with_header = |header: [u8; HEADER_LEN]| {
			let mut bytes = table.clone();
			bytes[..HEADER_LEN].copy_from_slice(&header);
			bytes
		};

		// A changed byte of the salt that its checksum was not made again for: damage.
		let mut rotted = header;
		rotted[40] ^= 1;
		assert_eq!(
			state_of(&with_header(rotted)),
			FileState::Damaged(EncryptionError::MalformedHeader)
		);

		// A version that this build does not know, which is told before the checksum is looked at.
		let mut later = header;
		later[4] = 2;
		assert_eq!(
			state_of(&with_header(later)),
			FileState::Damaged(EncryptionError::UnsupportedVersion)
		);

		// A header that is whole, with a checksum that fits, and says that the file is a log
		// segment.
		let mut other_kind = header;
		other_kind[6] = FileKind::Wal.id();
		assert_eq!(
			state_of(&with_header(with_fixed_crc(other_kind))),
			FileState::Damaged(EncryptionError::FileIdMismatch)
		);

		// And a cipher suite that nothing knows, with a checksum that fits.
		let mut other_suite = header;
		other_suite[5] = 9;
		assert_eq!(
			state_of(&with_header(with_fixed_crc(other_suite))),
			FileState::Damaged(EncryptionError::UnsupportedSuite)
		);
	}
}

#[test]
fn a_table_is_sealed_or_not_as_its_footer_says_whatever_its_first_bytes_are() {
	let dir = TempDir::new().unwrap();
	let (table, _, _) = sealed_table_and_manifest(CipherSuite::Aes256Gcm);
	let path = dir.path().join("00000000000000000009.sst");

	// The checksum type of the footer is the second byte of its 50: 2 for sealed blocks, 1 for
	// plain. A footer that says plain wins over a header at the start, as it does for the reader.
	let mut plain_footer = table.clone();
	let footer_at = plain_footer.len() - 50;
	assert_eq!(plain_footer[footer_at + 1], 2);
	plain_footer[footer_at + 1] = 1;
	fs::write(&path, &plain_footer).unwrap();
	assert_eq!(described(&path).state, FileState::Plaintext);

	// A footer of a table that is not a footer: a file with a header and then no table at all.
	let mut no_magic = table;
	let end = no_magic.len();
	no_magic[end - 1] ^= 0xFF;
	fs::write(&path, &no_magic).unwrap();
	assert!(matches!(described(&path).state, FileState::Sealed(_)));

	// Bytes that begin as nothing known and that no footer holds together.
	fs::write(&path, vec![0xAB; 400]).unwrap();
	assert_eq!(described(&path).state, FileState::Plaintext);
}

#[tokio::test]
async fn a_file_with_a_name_that_says_nothing_is_still_known_by_what_it_holds() {
	let dir = TempDir::new().unwrap();
	let root = dir.path().join("db");
	create(&root, manager(), CipherSuite::Aes256Gcm).await;
	let elsewhere = dir.path().join("copies");
	fs::create_dir_all(&elsewhere).unwrap();

	for (kind, path) in surfaces(&root) {
		let copy = elsewhere.join(format!("{kind:?}.bin"));
		fs::copy(&path, &copy).unwrap();
		let description = described(&copy);
		assert_eq!(description.kind, Some(kind), "{path:?}");
		assert!(description.is_encrypted(), "{path:?}");

		// The names that keep a file aside are what the first extension in them says.
		let kept = elsewhere.join(match kind {
			FileKind::Wal => "00000000000000000001.wal.corrupt.1",
			FileKind::Vlog => "00000000000000000001.vlog.torn",
			FileKind::Sst => "00000000000000000001.sst.bak",
			_ => "0.manifest.old",
		});
		fs::copy(&path, &kept).unwrap();
		let description = described(&kept);
		assert_eq!(description.kind, Some(kind), "{kept:?}");
		assert!(description.is_encrypted(), "{kept:?}");
	}

	// A file that holds nothing of the kinds that are sealed is nothing in particular.
	let other = elsewhere.join("notes.txt");
	fs::write(&other, b"a plaintext note, long enough to hold a header if it were one: 0123456789")
		.unwrap();
	let description = described(&other);
	assert_eq!((description.kind, description.state), (None, FileState::Plaintext));
}
