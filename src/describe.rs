//! What a file says about its encryption from its first bytes, without a key.
//!
//! Every sealed file starts, or for a table ends, with something that is readable by anyone: the
//! manifest with a magic and a header, a table with a header at its start and a footer that says
//! that its blocks are sealed, a value-log file with its own header and then the encryption
//! header, a segment of the write-ahead log with a header record. This reads those, and only
//! those, and reports the suite, the key id, the database and the file that the header names, and
//! the salt. None of it is secret, and none of it is authentic: the tag of a header is checked
//! with a key, which nothing here has.

#![allow(
	clippy::disallowed_methods,
	reason = "std-path tooling that reads the clear headers of the files of a directory on a disk, for a tool that holds no file system handle; it is not part of opening a database"
)]

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::{EncryptionError, Result};
use crate::sstable::table::{ChecksumType, Footer, TABLE_FULL_FOOTER_LENGTH};
use crate::tde::{
	is_sealed_manifest,
	CipherSuite,
	CryptoHeader,
	DbId,
	FileKind,
	HEADER_LEN,
	MANIFEST_PREFIX_LEN,
	SALT_LEN,
};
use crate::vlog::sealed::{is_sealed_start as vlog_is_sealed_start, SEALED_HEADER_LEN};
use crate::wal::sealed::starts_like_sealed as wal_starts_like_sealed;
use crate::Options;

/// What of the start of a file is read: the longest header of any kind of file, which is the one
/// of the value log.
const HEAD_LEN: usize = SEALED_HEADER_LEN;

/// Where the encryption header begins in each kind of file.
const MANIFEST_HEADER_AT: usize = MANIFEST_PREFIX_LEN - HEADER_LEN;
const TABLE_HEADER_AT: usize = 0;
const VLOG_HEADER_AT: usize = SEALED_HEADER_LEN - HEADER_LEN;
const WAL_HEADER_AT: usize = crate::wal::HEADER_SIZE;

/// What the encryption header of a file says, as it was read, without a key.
///
/// It is whatever the file says, and it can be wrong: nothing here has checked the tag of the
/// header, which needs the key of the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct CryptoSummary {
	/// The cipher suite that the blocks of the file are sealed with.
	pub suite: CipherSuite,
	/// The id of the key that the key of the file was derived from.
	pub key_id: u32,
	/// The database that the file says it belongs to.
	pub db_id: DbId,
	/// The file that the header says this is: the table id, the log file id or the segment
	/// number, and 0 for the manifest.
	pub file_id: u64,
	/// The salt that the key of the file was derived with. It is not secret: a key is made from
	/// it and a root that only the key manager has.
	pub salt: [u8; SALT_LEN],
}

impl CryptoSummary {
	fn of(header: &CryptoHeader) -> Self {
		Self {
			suite: header.identity.suite,
			key_id: header.identity.key_id,
			db_id: header.identity.db_id,
			file_id: header.identity.file_id,
			salt: header.salt,
		}
	}
}

/// What the start of a file says about whether it is encrypted.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FileState {
	/// Not encrypted: it does not begin the way an encrypted file of its kind does. Whether it is
	/// a well formed file of any kind is not examined.
	Plaintext,
	/// Encrypted, with a header that is well formed and that names the kind of file that this is.
	Sealed(CryptoSummary),
	/// Begins as an encrypted file does, and its header cannot be read or is not the header of
	/// this kind of file. A header that fails its checksum is damage, which is
	/// `MalformedHeader`; a header of a later format is `UnsupportedVersion` or
	/// `UnsupportedSuite`; one that names another kind of file is `FileIdMismatch`.
	Damaged(EncryptionError),
	/// Empty, zeros, or cut off before its header is whole: what a write that did not finish
	/// leaves, and a file that holds nothing a key was used for.
	Unfinished,
}

/// What a file is, as far as its first bytes say, without a key. See [`describe_file`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FileDescription {
	/// The path that was described.
	pub path: std::path::PathBuf,
	/// The size of the file in bytes.
	pub file_size: u64,
	/// The kind of file: what the name says for a file that is not encrypted, and what the header
	/// says for one that is. `None` for a file that is neither a name nor a header of any kind.
	pub kind: Option<FileKind>,
	/// Whether the file is encrypted, and what its header says.
	pub state: FileState,
}

impl FileDescription {
	/// Whether the file is encrypted or at least begins as an encrypted file does.
	pub fn is_encrypted(&self) -> bool {
		matches!(self.state, FileState::Sealed(_) | FileState::Damaged(_))
	}

	/// What the header of the file says, if it is encrypted and the header is well formed.
	pub fn crypto(&self) -> Option<&CryptoSummary> {
		match &self.state {
			FileState::Sealed(summary) => Some(summary),
			_ => None,
		}
	}
}

/// The kind of file that a name is, going by the extensions in it: a table is `.sst`, a value-log
/// file `.vlog`, a segment of the write-ahead log `.wal` and the manifest `.manifest`. The names
/// that are made of more than that, such as `.wal.corrupt` for a segment that was kept aside and
/// `.vlog.torn`, are what the first of them says.
pub(crate) fn kind_from_name(path: &Path) -> Option<FileKind> {
	let name = path.file_name()?.to_str()?;
	name.split('.').skip(1).find_map(|part| match part {
		"sst" => Some(FileKind::Sst),
		"wal" => Some(FileKind::Wal),
		"vlog" => Some(FileKind::Vlog),
		"manifest" => Some(FileKind::Manifest),
		_ => None,
	})
}

/// The encryption header that begins at `at` in `head`, as a state: well formed and of the kind
/// that it is looked for as, damaged, or cut off.
fn sealed_state(head: &[u8], at: usize, kind: FileKind) -> FileState {
	let Some(bytes) = at.checked_add(HEADER_LEN).and_then(|end| head.get(at..end)) else {
		return FileState::Unfinished;
	};
	match CryptoHeader::decode(bytes) {
		Ok(header) if header.identity.file_kind == kind => {
			FileState::Sealed(CryptoSummary::of(&header))
		}
		Ok(_) => FileState::Damaged(EncryptionError::FileIdMismatch),
		Err(e) => FileState::Damaged(e),
	}
}

/// Describes the table whose first bytes are `head` and whose size is `file_size`: its footer says
/// whether its blocks are sealed, and a table that was cut off before its footer is told by its
/// header.
fn describe_table(file: &mut File, file_size: u64, head: &[u8]) -> Result<FileState> {
	let footer_len = TABLE_FULL_FOOTER_LENGTH as u64;
	if file_size >= footer_len {
		let mut buf = [0u8; TABLE_FULL_FOOTER_LENGTH];
		file.seek(SeekFrom::Start(file_size - footer_len))?;
		file.read_exact(&mut buf)?;
		if let Ok(footer) = Footer::decode(&buf) {
			return Ok(match footer.checksum {
				ChecksumType::SealedCRC32c => sealed_state(head, TABLE_HEADER_AT, FileKind::Sst),
				ChecksumType::CRC32c => FileState::Plaintext,
			});
		}
	}
	// No footer that can be read: a file whose first bytes are an encryption header was a sealed
	// table that did not get to its footer, and holds blocks sealed under that header's key.
	if CryptoHeader::has_magic(head) {
		return Ok(sealed_state(head, TABLE_HEADER_AT, FileKind::Sst));
	}
	Ok(FileState::Plaintext)
}

/// Reads the first bytes of the file at `path` and says what they are, see [`describe_file`].
fn read_description(path: &Path) -> Result<FileDescription> {
	let mut file = File::open(path)?;
	let file_size = file.metadata()?.len();
	let mut head = Vec::with_capacity(HEAD_LEN);
	(&mut file).take(HEAD_LEN as u64).read_to_end(&mut head)?;

	let named = kind_from_name(path);
	let (kind, state) = if head.iter().all(|&byte| byte == 0) {
		// Nothing, or zeros: no kind of file begins like that.
		(named, FileState::Unfinished)
	} else if is_sealed_manifest(&head) {
		// The magic of the manifest is its own, so whatever the file is called, it is one.
		(Some(FileKind::Manifest), sealed_state(&head, MANIFEST_HEADER_AT, FileKind::Manifest))
	} else {
		match named {
			Some(FileKind::Manifest) => (named, FileState::Plaintext),
			Some(FileKind::Vlog) => {
				let state = if vlog_is_sealed_start(&head) {
					sealed_state(&head, VLOG_HEADER_AT, FileKind::Vlog)
				} else {
					FileState::Plaintext
				};
				(named, state)
			}
			Some(FileKind::Wal) => {
				let state = if wal_starts_like_sealed(&head) {
					sealed_state(&head, WAL_HEADER_AT, FileKind::Wal)
				} else {
					FileState::Plaintext
				};
				(named, state)
			}
			Some(FileKind::Sst) => (named, describe_table(&mut file, file_size, &head)?),
			// A name that says nothing: only what cannot be taken for anything else counts. A
			// value-log file begins with its magic and the version of a sealed one, a table
			// ends in a footer that says sealed and begins with a header, and a segment has the
			// type of its header record and the magic of the encryption header after it.
			_ if vlog_is_sealed_start(&head) => {
				(Some(FileKind::Vlog), sealed_state(&head, VLOG_HEADER_AT, FileKind::Vlog))
			}
			_ if CryptoHeader::has_magic(&head) => {
				match describe_table(&mut file, file_size, &head)? {
					state @ (FileState::Sealed(_) | FileState::Damaged(_)) => {
						(Some(FileKind::Sst), state)
					}
					_ => (None, FileState::Plaintext),
				}
			}
			_ if head.get(WAL_HEADER_AT - 1)
				== Some(&(crate::wal::RecordType::SegmentHeader as u8))
				&& head.get(WAL_HEADER_AT..WAL_HEADER_AT + 4) == Some(&b"SKTD"[..]) =>
			{
				(Some(FileKind::Wal), sealed_state(&head, WAL_HEADER_AT, FileKind::Wal))
			}
			_ => (None, FileState::Plaintext),
		}
	};
	Ok(FileDescription {
		path: path.to_path_buf(),
		file_size,
		kind,
		state,
	})
}

/// Says what the file at `path` is and whether it is encrypted, from its first bytes and, for a
/// table, its footer, without a key.
///
/// The kind of file is told by its name when it is not encrypted (`.sst`, `.vlog`, `.wal`,
/// `.manifest`) and by its header when it is, and a file that the name says is of one kind and
/// whose header says that it is of another is [`FileState::Damaged`] with `FileIdMismatch`. For
/// an encrypted file the answer holds the suite, the key id, the database, the file id and the
/// salt that its header names. Nothing is authenticated and no secret is read, and a file that
/// is damaged, of a later format or cut off is described as that, never with an error: an error is
/// only for a file that cannot be read at all.
pub fn describe_file<P: AsRef<Path>>(path: P) -> Result<FileDescription> {
	read_description(path.as_ref())
}

/// Whether the file or database at `path` is encrypted, without a key: for a file, whether
/// [`describe_file`] says it is, and for a directory, whether its manifest is. A directory that
/// has no manifest is encrypted if any file in it, or in its `wal`, `sstables` or `vlog`
/// directory, is.
///
/// It reads only the first bytes of the files that it looks at, and only the manifest of a
/// database that has one.
pub fn is_encrypted<P: AsRef<Path>>(path: P) -> Result<bool> {
	let path = path.as_ref();
	if !path.is_dir() {
		return Ok(read_description(path)?.is_encrypted());
	}

	let opts = Options {
		path: path.to_path_buf(),
		..Default::default()
	};
	let manifest = opts.manifest_file_path(0);
	if manifest.is_file() {
		return Ok(read_description(&manifest)?.is_encrypted());
	}

	for dir in [path.to_path_buf(), opts.wal_dir(), opts.sstable_dir(), opts.vlog_dir()] {
		let Ok(entries) = std::fs::read_dir(&dir) else {
			continue;
		};
		for entry in entries.flatten() {
			let file = entry.path();
			// A file that was removed since it was listed is no longer there to be encrypted.
			if file.is_file()
				&& kind_from_name(&file).is_some()
				&& read_description(&file).is_ok_and(|described| described.is_encrypted())
			{
				return Ok(true);
			}
		}
	}
	Ok(false)
}
