//! The scrub of a database: every block of every table, of all six kinds, and what is said of a
//! table that cannot be read.
//!
//! The expected number of blocks of each kind is counted by the walk of `sstable_encryption_tests`,
//! which follows the handles from the footer with a reader of its own, and not by the code that is
//! tested. Tables are small, since sealing in an unoptimised build is slow, and a table that is
//! written here has several blocks of each kind.

use std::fs;
use std::sync::Arc;

use tempfile::TempDir;

use super::encryption_open_tests::{create, files_with_extension, manager};
use super::sstable_encryption_tests::{
	context,
	first_of,
	flip,
	many_points,
	options,
	walk,
	write_table,
	KEYS,
	TABLE_ID,
};
use crate::error::EncryptionError;
use crate::inspect::{
	dump_sst_with_options,
	scrub_db,
	scrub_db_with_options,
	BlockCounts,
	ScrubReport,
};
use crate::sstable::block::BlockHandle;
use crate::sstable::block_io::{BlockKind, TrailerLayout};
use crate::tde::{
	CipherSuite,
	DbId,
	EncryptionContext,
	KeyManager,
	SoftwareKeyManager,
	HEADER_LEN,
	ROOT_KEY_LEN,
};
use crate::{CompressionType, Error, Options};

const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

const ALL_KINDS: [BlockKind; 6] = [
	BlockKind::Data,
	BlockKind::IndexPartition,
	BlockKind::TopLevelIndex,
	BlockKind::FilterPartition,
	BlockKind::FilterIndex,
	BlockKind::MetaIndex,
];

/// Options that carry the key that the tables of these tests were sealed under.
fn keyed(suite: CipherSuite) -> Options {
	keyed_with(manager(), suite)
}

fn keyed_with(key_manager: Arc<dyn KeyManager>, suite: CipherSuite) -> Options {
	Options::new().with_encryption(key_manager, suite)
}

/// A directory that holds a database whose only files are the tables given, named as a database
/// names them. It has no manifest.
fn database_of(tables: &[(u64, &[u8])]) -> TempDir {
	let dir = TempDir::new().unwrap();
	let sstables = dir.path().join("sstables");
	fs::create_dir_all(&sstables).unwrap();
	for (id, bytes) in tables {
		fs::write(sstables.join(format!("{id:020}.sst")), bytes).unwrap();
	}
	dir
}

/// The report of a scrub of a database whose only table is `bytes`, as table `id`.
fn scrub_one(bytes: &[u8], id: u64, options: &Options) -> ScrubReport {
	let dir = database_of(&[(id, bytes)]);
	scrub_db_with_options(dir.path(), options).unwrap()
}

/// How many blocks of each kind a table has, from the blocks that its walk found.
fn counts_of(blocks: &[(BlockKind, BlockHandle)]) -> BlockCounts {
	let of = |kind: BlockKind| blocks.iter().filter(|(found, _)| *found == kind).count();
	BlockCounts {
		data: of(BlockKind::Data),
		index_partitions: of(BlockKind::IndexPartition),
		top_level_index: of(BlockKind::TopLevelIndex),
		filter_partitions: of(BlockKind::FilterPartition),
		filter_index: of(BlockKind::FilterIndex),
		meta_index: of(BlockKind::MetaIndex),
	}
}

/// What a report says of the tables: how many were verified, damaged and not verified.
fn outcome(report: &ScrubReport) -> (usize, usize, usize) {
	(report.verified_tables, report.corrupt_blocks, report.not_verified_tables)
}

#[test]
fn a_scrub_counts_the_blocks_of_all_six_kinds_of_a_plain_and_of_a_sealed_table() {
	for suite in SUITES {
		for compression in [CompressionType::None, CompressionType::ZstdCompression] {
			let ctx = context(suite);
			let sealed_opts = options(Some(&ctx), compression, true);
			let plain_opts = options(None, compression, true);
			let sealed = write_table(&sealed_opts, TABLE_ID, &many_points(KEYS));
			let plain = write_table(&plain_opts, TABLE_ID, &many_points(KEYS));
			let in_sealed = counts_of(&walk(&sealed_opts, &sealed, TABLE_ID));
			let in_plain = counts_of(&walk(&plain_opts, &plain, TABLE_ID));

			// The tables have every kind of block, and several of the kinds that a table has many
			// of.
			for counts in [&in_sealed, &in_plain] {
				assert_eq!(counts.top_level_index, 1, "{counts:?}");
				assert_eq!(counts.meta_index, 1, "{counts:?}");
				assert_eq!(counts.filter_index, 1, "{counts:?}");
				assert!(counts.data > 1 && counts.index_partitions > 1, "{counts:?}");
				assert!(counts.filter_partitions > 1, "{counts:?}");
			}

			let scrubbed = scrub_one(&sealed, TABLE_ID, &keyed(suite));
			assert_eq!(outcome(&scrubbed), (1, 0, 0), "{suite:?} {compression:?}");
			assert_eq!(scrubbed.blocks, in_sealed, "{suite:?} {compression:?}");
			assert_eq!(scrubbed.total_blocks, in_sealed.total());
			assert!(scrubbed.errors.is_empty() && scrubbed.total_tables == 1, "{scrubbed:?}");

			// A table in the clear is read to its end by a scrub that has no key, as it always was,
			// and now through every kind of block and not only the data blocks.
			let scrubbed = scrub_one(&plain, TABLE_ID, &Options::default());
			assert_eq!(outcome(&scrubbed), (1, 0, 0), "{compression:?}");
			assert_eq!(scrubbed.blocks, in_plain, "{compression:?}");
			assert_eq!(scrubbed.total_blocks, in_plain.total());
			assert!(scrubbed.errors.is_empty(), "{scrubbed:?}");
		}
	}
}

#[test]
fn the_dump_of_a_table_says_how_many_blocks_of_each_kind_it_verified() {
	let ctx = context(CipherSuite::XChaCha20Poly1305);
	let opts = options(Some(&ctx), CompressionType::None, true);
	let file = write_table(&opts, TABLE_ID, &many_points(KEYS));
	let expected = counts_of(&walk(&opts, &file, TABLE_ID));
	let dir = database_of(&[(TABLE_ID, &file)]);
	let path = dir.path().join("sstables").join(format!("{TABLE_ID:020}.sst"));
	let options = keyed(CipherSuite::XChaCha20Poly1305);

	let info = dump_sst_with_options(&path, false, true, &options).unwrap();
	assert_eq!(info.verified, Some(expected));
	assert_eq!(info.verified_blocks, Some(expected.total()));
	assert_eq!(info.data_blocks_count, expected.data as u64);

	let info = dump_sst_with_options(&path, false, false, &options).unwrap();
	assert_eq!((info.verified, info.verified_blocks), (None, None));
}

#[test]
fn a_block_of_any_kind_that_does_not_authenticate_is_damage_with_the_key_and_unknown_without_it() {
	for suite in SUITES {
		let ctx = context(suite);
		let opts = options(Some(&ctx), CompressionType::None, true);
		let file = write_table(&opts, TABLE_ID, &many_points(KEYS));
		let blocks = walk(&opts, &file, TABLE_ID);
		for kind in ALL_KINDS {
			let handle = first_of(&blocks, kind);
			let what = format!("{suite:?}, a {kind:?} block");

			// Changed by someone who made the checksum fit again, which only the seal can find.
			let forged = flip(&file, &handle, 0, TrailerLayout::Sealed, true);
			let dir = database_of(&[(TABLE_ID, &forged)]);
			let with_key = scrub_db_with_options(dir.path(), &keyed(suite)).unwrap();
			assert_eq!(outcome(&with_key), (0, 1, 0), "{what}: {with_key:?}");
			assert_eq!(with_key.errors.len(), 1, "{what}");
			assert!(with_key.errors[0].contains("authentication failed"), "{what}: {with_key:?}");
			assert_eq!(with_key.blocks, BlockCounts::default(), "{what}: nothing was counted");

			// Without the key nothing can be said of the table, and what is not said is not that
			// it is damaged.
			let keyless = scrub_db(dir.path()).unwrap();
			assert_eq!(outcome(&keyless), (0, 0, 1), "{what}: {keyless:?}");

			// Changed so that the checksum no longer fits, which is what damage looks like: found
			// by the key's scrub as damage, and still not said of a table that is not read.
			let rotted = flip(&file, &handle, 0, TrailerLayout::Sealed, false);
			let dir = database_of(&[(TABLE_ID, &rotted)]);
			let with_key = scrub_db_with_options(dir.path(), &keyed(suite)).unwrap();
			assert_eq!(outcome(&with_key), (0, 1, 0), "{what}: {with_key:?}");
			let keyless = scrub_db(dir.path()).unwrap();
			assert_eq!(outcome(&keyless), (0, 0, 1), "{what}: {keyless:?}");
		}
	}
}

#[test]
fn a_damaged_block_of_any_kind_of_a_plain_table_is_found() {
	let opts = options(None, CompressionType::None, true);
	let file = write_table(&opts, TABLE_ID, &many_points(KEYS));
	let blocks = walk(&opts, &file, TABLE_ID);
	for kind in ALL_KINDS {
		let handle = first_of(&blocks, kind);
		let rotted = flip(&file, &handle, 0, TrailerLayout::Plain, false);
		let report = scrub_one(&rotted, TABLE_ID, &Options::default());
		assert_eq!(outcome(&report), (0, 1, 0), "a {kind:?} block: {report:?}");
		assert_eq!(report.errors.len(), 1, "a {kind:?} block");
	}
}

/// The sealed table that the cases below change, with the options that read it.
fn sealed_table(suite: CipherSuite) -> Vec<u8> {
	let ctx = context(suite);
	write_table(&options(Some(&ctx), CompressionType::None, true), TABLE_ID, &many_points(KEYS))
}

/// `file` with another header: the first bytes of a sealed table.
fn with_header(file: &[u8], header: [u8; HEADER_LEN]) -> Vec<u8> {
	let mut changed = file.to_vec();
	changed[..HEADER_LEN].copy_from_slice(&header);
	changed
}

/// The header with its checksum made again, which is its last four bytes and covers the ones
/// before them.
fn with_fixed_crc(mut header: [u8; HEADER_LEN]) -> [u8; HEADER_LEN] {
	let crc = crc32fast::hash(&header[..HEADER_LEN - 4]);
	header[HEADER_LEN - 4..].copy_from_slice(&crc.to_be_bytes());
	header
}

#[test]
fn a_table_that_cannot_be_read_is_not_verified_and_is_not_said_to_be_damaged_unless_it_is() {
	for suite in SUITES {
		let file = sealed_table(suite);
		let header: [u8; HEADER_LEN] = file[..HEADER_LEN].try_into().unwrap();
		let said = |report: &ScrubReport| report.errors.first().cloned().unwrap_or_default();

		// A key that does not open it, or that was retired.
		let other: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new([0x43; ROOT_KEY_LEN]));
		let wrong = scrub_one(&file, TABLE_ID, &keyed_with(other, suite));
		assert_eq!(outcome(&wrong), (0, 0, 1), "{suite:?}: {wrong:?}");
		assert!(said(&wrong).contains("does not open it"), "{}", said(&wrong));

		let retired = SoftwareKeyManager::new([0x42; ROOT_KEY_LEN]);
		retired.add_key(2, [0x44; ROOT_KEY_LEN]).unwrap();
		retired.set_active_key_id(2).unwrap();
		retired.retire_key(1).unwrap();
		let retired = scrub_one(&file, TABLE_ID, &keyed_with(Arc::new(retired), suite));
		assert_eq!(outcome(&retired), (0, 0, 1), "{suite:?}: {retired:?}");
		assert!(said(&retired).contains("does not open it"), "{}", said(&retired));

		// A header of a later version, and of a suite that nothing knows, with its checksum right.
		let mut later = header;
		later[4] = 2;
		let mut other_suite = header;
		other_suite[5] = 9;
		for (what, bytes) in [
			("a later version", with_header(&file, later)),
			("another suite", with_header(&file, with_fixed_crc(other_suite))),
		] {
			let report = scrub_one(&bytes, TABLE_ID, &keyed(suite));
			assert_eq!(outcome(&report), (0, 0, 1), "{suite:?}, {what}: {report:?}");
			assert!(said(&report).contains("newer format"), "{what}: {}", said(&report));
		}

		// A header that does not match its checksum is damage.
		let mut rotted = header;
		rotted[40] ^= 1;
		let report = scrub_one(&with_header(&file, rotted), TABLE_ID, &keyed(suite));
		assert_eq!(outcome(&report), (0, 1, 0), "{suite:?}: {report:?}");

		// A table of another database than the one that the caller says the database is.
		let km: Arc<dyn KeyManager> = manager();
		let expected = EncryptionContext::new(km, DbId::from_bytes([0x11; 16]), suite);
		let expecting = Options::new().with_encryption_context(Arc::new(expected));
		let report = scrub_one(&file, TABLE_ID, &expecting);
		assert_eq!(outcome(&report), (0, 0, 1), "{suite:?}: {report:?}");
		assert!(said(&report).contains("another database"), "{}", said(&report));

		// A table that is not the one that its name says: a copy, or a file that was renamed.
		let report = scrub_one(&file, TABLE_ID + 1, &keyed(suite));
		assert_eq!(outcome(&report), (0, 0, 1), "{suite:?}: {report:?}");
		assert!(said(&report).contains("file name says"), "{}", said(&report));

		// A table in the clear, where a key was given.
		let plain =
			write_table(&options(None, CompressionType::None, true), TABLE_ID, &many_points(KEYS));
		let report = scrub_one(&plain, TABLE_ID, &keyed(suite));
		assert_eq!(outcome(&report), (0, 0, 1), "{suite:?}: {report:?}");
		assert!(said(&report).contains("not encrypted"), "{}", said(&report));

		// And no key at all, which is what a tool that was given none has.
		let keyless = scrub_one(&file, TABLE_ID, &Options::default());
		assert_eq!(outcome(&keyless), (0, 0, 1), "{suite:?}: {keyless:?}");
		assert!(said(&keyless).contains("not verified"), "{}", said(&keyless));
		assert!(said(&keyless).contains("without a key"), "{}", said(&keyless));
	}
}

#[tokio::test]
async fn a_database_that_was_written_by_a_tree_is_verified_with_its_key_and_not_without_it() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		create(dir.path(), manager(), suite).await;
		let tables = files_with_extension(&dir.path().join("sstables"), "sst").len();
		assert!(tables >= 1);

		let report = scrub_db_with_options(dir.path(), &keyed(suite)).unwrap();
		assert_eq!(report.total_tables, tables, "{suite:?}");
		assert_eq!(outcome(&report), (tables, 0, 0), "{suite:?}: {report:?}");
		assert!(report.total_blocks > 0 && report.total_blocks == report.blocks.total());
		assert!(report.errors.is_empty(), "{report:?}");
		// The table has every kind of block that a table of a tree has: the data of a few keys.
		assert!(report.blocks.data >= 1 && report.blocks.meta_index >= 1, "{:?}", report.blocks);

		// A scrub that has no key does not say that the database is whole, nor that it is not.
		let keyless = scrub_db(dir.path()).unwrap();
		assert_eq!(outcome(&keyless), (0, 0, tables), "{suite:?}: {keyless:?}");
		assert_eq!(keyless.errors.len(), tables);
		assert_eq!(keyless.total_blocks, 0);

		// A key that does not open the manifest is the error of the call, before any table is read.
		let other: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new([0x43; ROOT_KEY_LEN]));
		match scrub_db_with_options(dir.path(), &keyed_with(other, suite)) {
			Err(Error::Encryption(EncryptionError::WrongKey)) => {}
			result => panic!("{suite:?}: expected a wrong key, got {result:?}"),
		}
	}
}
