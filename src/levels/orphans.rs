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
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::error::{EncryptionError, Error, Result};
use crate::tde::{CryptoHeader, EncryptionContext, DB_ID_LEN, HEADER_LEN, HEADER_MAGIC};
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
	let dir = opts.sstable_dir();
	if !dir.exists() {
		return Ok(Vec::new());
	}
	let mut orphans = Vec::new();
	for entry in std::fs::read_dir(&dir)? {
		let entry = entry?;
		let name = entry.file_name().to_string_lossy().into_owned();
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
		let path = entry.path();
		if let Some(context) = opts.encryption_context() {
			check_unlisted_table(context, &path)?;
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
fn check_unlisted_table(context: &EncryptionContext, path: &Path) -> Result<()> {
	let mut head = Vec::with_capacity(HEADER_LEN);
	match File::open(path) {
		Ok(file) => {
			file.take(HEADER_LEN as u64).read_to_end(&mut head)?;
		}
		// Gone since the directory was listed, which is a clean-up that is already done.
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
		Err(e) => return Err(e.into()),
	}

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
