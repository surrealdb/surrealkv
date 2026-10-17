//! Which table files the start-up clean-up may delete.
//!
//! A table file that the manifest does not list is normally what a flush or a compaction that did
//! not finish left behind, and the clean-up removes it by its name. In a database that is
//! encrypted that is not enough: a table of another database that shares the key, or a table in the
//! clear, is also a file that the manifest does not list, and nothing about its name says that it
//! is not this database's. Deleting it would destroy data that the operator put there, or restored
//! from the wrong place, without anyone having read it.
//!
//! So each unlisted file is read from its first bytes, which need no key, before any of them is
//! deleted: a table of this database is cleaned as it always was, and any other makes the open fail
//! with the error that says what it is, deleting nothing.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::error::{EncryptionError, Error, Result};
use crate::tde::{CryptoHeader, EncryptionContext, DB_ID_LEN, HEADER_LEN, HEADER_MAGIC};
use crate::vfs::Fs;
use crate::Options;

/// The offset of the database id in the encryption header of a file.
const DB_ID_OFFSET: usize = 16;

/// The table files in the table directory of `opts` that the manifest does not list, `live` being
/// the ids it does, with the id of each. Reads only.
///
/// In a database that is encrypted, every one of them is read from its first bytes, and if one of
/// them is not a file that this database could have left, the answer is the error for it and no
/// file is named for deletion: see `check_unlisted_table`. A database that is not encrypted is
/// answered by the names alone, as it always was.
pub(crate) fn plan_cleanup(opts: &Options, live: &HashSet<u64>) -> Result<Vec<(u64, PathBuf)>> {
	plan_cleanup_in(&*opts.fs, opts, live)
}

/// [`plan_cleanup`] over `fs`: the directory is listed, and the files are read, there.
pub(crate) fn plan_cleanup_in(
	fs: &dyn Fs,
	opts: &Options,
	live: &HashSet<u64>,
) -> Result<Vec<(u64, PathBuf)>> {
	let dir = opts.sstable_dir();
	if !fs.exists(&dir) {
		return Ok(Vec::new());
	}
	let mut orphans = Vec::new();
	for entry in fs.read_dir(&dir)? {
		let name = entry.name.to_string_lossy().into_owned();
		// The name of a table file: {id:020}.sst
		if !(name.ends_with(".sst") && name.len() == 24) {
			continue;
		}
		let Ok(id) = name[..20].parse::<u64>() else {
			continue;
		};
		if live.contains(&id) {
			continue;
		}
		let path = dir.join(&entry.name);
		if let Some(context) = opts.encryption_context() {
			check_unlisted_table_in(fs, context, &path)?;
		}
		orphans.push((id, path));
	}
	Ok(orphans)
}

/// Says whether the file at `path`, which the manifest does not list, may be taken for a leftover
/// of this database, from its first bytes and without a key.
///
/// Fine: a file with nothing in it, or with only zeros, which is what a crash leaves when the file
/// was made and its first write was not; a start of the encryption header that is cut short, which
/// holds no block; and a whole header that names this database. Whatever it was sealed under, a
/// file that names this database is this database's, an older copy of a table included.
///
/// Refused, with the error that names what it is: a sealed table that names another database
/// (`DbIdMismatch`), including one whose header is cut short after the database id, a header that
/// cannot be read (`MalformedHeader`, `UnsupportedVersion`, `UnsupportedSuite`), and a file that
/// does not start as a sealed table at all, which is in the clear in a database that is not
/// (`PlaintextInEncryptedDb`). The files that are refused are left as they are.
#[cfg(test)]
fn check_unlisted_table(context: &EncryptionContext, path: &Path) -> Result<()> {
	check_unlisted_table_in(&*crate::vfs::default_fs(), context, path)
}

/// [`check_unlisted_table`] over `fs`.
fn check_unlisted_table_in(fs: &dyn Fs, context: &EncryptionContext, path: &Path) -> Result<()> {
	// Gone since the directory was listed, which is a clean-up that is already done.
	let Some(head) = fs.read_prefix(path, HEADER_LEN)? else {
		return Ok(());
	};

	if head.iter().all(|&byte| byte == 0) {
		return Ok(());
	}
	if !head.starts_with(&HEADER_MAGIC) {
		// Less than the magic, and all of it that is there is its start: a first write that was
		// cut short.
		if head.len() < HEADER_MAGIC.len() && HEADER_MAGIC.starts_with(&head) {
			return Ok(());
		}
		return Err(EncryptionError::PlaintextInEncryptedDb.into());
	}

	if head.len() < HEADER_LEN {
		return match head.get(DB_ID_OFFSET..DB_ID_OFFSET + DB_ID_LEN) {
			Some(named) if named != context.db_id().as_bytes() => {
				Err(EncryptionError::DbIdMismatch.into())
			}
			_ => Ok(()),
		};
	}
	let header = CryptoHeader::decode(&head).map_err(Error::from)?;
	if header.identity.db_id != context.db_id() {
		return Err(EncryptionError::DbIdMismatch.into());
	}
	Ok(())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use std::sync::Arc;

	use tempfile::TempDir;

	use super::*;
	use crate::tde::{CipherSuite, DbId, FileKind, SoftwareKeyManager};

	fn context(db: u8) -> EncryptionContext {
		EncryptionContext::new(
			Arc::new(SoftwareKeyManager::new([0x31; 32])),
			DbId::from_bytes([db; 16]),
			CipherSuite::XChaCha20Poly1305,
		)
	}

	/// The header that a table of the database `db` starts with.
	fn header_of(db: u8) -> [u8; HEADER_LEN] {
		let (header, _) = context(db).create_file(FileKind::Sst, 9, &[]).unwrap();
		header.encode()
	}

	/// What `check_unlisted_table` says about a file that holds `bytes`, for the database 0xD1.
	fn verdict(bytes: &[u8]) -> Option<EncryptionError> {
		let dir = TempDir::new().unwrap();
		let path = dir.path().join("00000000000000000009.sst");
		std::fs::write(&path, bytes).unwrap();
		match check_unlisted_table(&context(0xD1), &path) {
			Ok(()) => None,
			Err(Error::Encryption(error)) => Some(error),
			Err(other) => panic!("not an encryption error: {other:?}"),
		}
	}

	#[test]
	fn what_a_crash_leaves_behind_is_this_databases_to_clean() {
		let whole = header_of(0xD1);
		let with_blocks = [whole.as_slice(), &[0xEE; 50]].concat();
		let cases: Vec<(&str, Vec<u8>)> = vec![
			("an empty file", Vec::new()),
			("zeros", vec![0; 300]),
			("the first byte of the magic", HEADER_MAGIC[..1].to_vec()),
			("most of the magic", HEADER_MAGIC[..3].to_vec()),
			("a header cut before the database", whole[..20].to_vec()),
			("a header cut one byte before the end of the database", whole[..31].to_vec()),
			("a header cut after the database", whole[..40].to_vec()),
			("a whole header", whole.to_vec()),
			("a header and blocks", with_blocks),
		];
		for (what, bytes) in cases {
			assert_eq!(verdict(&bytes), None, "{what}");
		}
	}

	#[test]
	fn anything_else_is_refused_with_the_error_that_names_it() {
		let theirs = header_of(0xD2);
		let mut rotted = header_of(0xD1);
		// A byte of the salt, which the checksum of the header covers.
		rotted[40] ^= 1;
		let mut later = header_of(0xD1);
		// The version of the format, which is told before the checksum is looked at.
		later[4] = 2;
		let cases: Vec<(&str, Vec<u8>, EncryptionError)> = vec![
			("another database", theirs.to_vec(), EncryptionError::DbIdMismatch),
			(
				"another database, cut just after its id",
				theirs[..32].to_vec(),
				EncryptionError::DbIdMismatch,
			),
			("a header with a changed bit", rotted.to_vec(), EncryptionError::MalformedHeader),
			("a header of another version", later.to_vec(), EncryptionError::UnsupportedVersion),
			(
				"a file in the clear",
				b"the first block of a table in the clear".to_vec(),
				EncryptionError::PlaintextInEncryptedDb,
			),
			(
				"two bytes that are not the magic",
				b"SX".to_vec(),
				EncryptionError::PlaintextInEncryptedDb,
			),
			(
				"a magic that is not the magic",
				b"SKTX".to_vec(),
				EncryptionError::PlaintextInEncryptedDb,
			),
		];
		for (what, bytes, wanted) in cases {
			assert_eq!(verdict(&bytes), Some(wanted), "{what}");
		}
	}

	#[test]
	fn a_file_that_is_gone_is_a_clean_up_that_is_done() {
		let dir = TempDir::new().unwrap();
		let gone = dir.path().join("00000000000000000009.sst");
		assert!(check_unlisted_table(&context(0xD1), &gone).is_ok());
	}

	#[test]
	fn the_plan_names_what_this_database_left_and_refuses_everything_if_one_file_is_not_its_own() {
		if crate::test::tfs::skip_listed() {
			return;
		}
		let dir = TempDir::new().unwrap();
		let opts = Options {
			path: dir.path().to_path_buf(),
			..Default::default()
		}
		.with_encryption_context(Arc::new(context(0xD1)));
		std::fs::create_dir_all(opts.sstable_dir()).unwrap();
		let table = |id: u64| opts.sstable_dir().join(format!("{id:020}.sst"));
		let planned = |opts: &Options, live: &HashSet<u64>| -> Vec<u64> {
			let mut ids: Vec<u64> =
				plan_cleanup(opts, live).unwrap().into_iter().map(|(id, _)| id).collect();
			ids.sort_unstable();
			ids
		};

		// Table 1 is listed. Tables 2 and 3 are what a flush that did not finish leaves, and
		// a file that is not named like a table is nobody's business.
		std::fs::write(table(1), header_of(0xD1)).unwrap();
		std::fs::write(table(2), header_of(0xD1)).unwrap();
		std::fs::write(table(3), b"").unwrap();
		std::fs::write(opts.sstable_dir().join("notes.txt"), b"not a table").unwrap();
		let live = HashSet::from([1u64]);
		assert_eq!(planned(&opts, &live), vec![2, 3]);

		// One table of another database, and nothing is planned for deletion, the tables that
		// are this database's included. Nothing was deleted by asking either.
		std::fs::write(table(4), header_of(0xD2)).unwrap();
		let refused = plan_cleanup(&opts, &live).unwrap_err();
		assert!(matches!(refused, Error::Encryption(EncryptionError::DbIdMismatch)), "{refused:?}");
		for id in 1..=4 {
			assert!(table(id).exists(), "{id}");
		}

		// A database that is not encrypted is answered by the names, as it always was.
		let plain = Options {
			path: dir.path().to_path_buf(),
			..Default::default()
		};
		assert_eq!(planned(&plain, &live), vec![2, 3, 4]);
	}
}
