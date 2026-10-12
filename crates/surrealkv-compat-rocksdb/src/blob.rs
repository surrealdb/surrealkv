//! Blob files (`NNNNNN.blob`): values of the size that RocksDB keeps out of the tables. A table
//! holds a blob index in place of the value.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use crate::coding::{crc32c, crc32c_extend, fixed32, fixed64, mask_crc, varint64};
use crate::compression;
use crate::error::{corrupt, unsupported, Result};

const BLOB_MAGIC: u32 = 2_395_959;
const HEADER_SIZE: u64 = 30;
const RECORD_HEADER_SIZE: usize = 32;
const INDEX_INLINED_TTL: u8 = 0;
const INDEX_BLOB: u8 = 1;
const INDEX_BLOB_TTL: u8 = 2;
/// How many blob files stay open at once.
const MAX_OPEN_FILES: usize = 32;

/// Where a value lives in a blob file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRef {
	pub file_number: u64,
	/// The offset of the value itself (not of its record header).
	pub offset: u64,
	/// The size of the stored, possibly compressed, value.
	pub size: u64,
	pub compression: u8,
}

/// Decodes the value of an entry of type `BlobIndex`.
pub(crate) fn decode_blob_index(value: &[u8]) -> Result<BlobRef> {
	let kind = *value.first().ok_or_else(|| corrupt("an empty blob index"))?;
	match kind {
		INDEX_BLOB => {
			let mut pos = 1;
			let file_number = varint64(value, &mut pos)?;
			let offset = varint64(value, &mut pos)?;
			let size = varint64(value, &mut pos)?;
			let compression =
				*value.get(pos).ok_or_else(|| corrupt("a blob index is cut short"))?;
			Ok(BlobRef {
				file_number,
				offset,
				size,
				compression,
			})
		}
		INDEX_INLINED_TTL | INDEX_BLOB_TTL => {
			Err(unsupported("blob values with an expiration time"))
		}
		other => Err(corrupt(format!("a blob index of unknown kind {other}"))),
	}
}

/// Reads values out of the blob files of a database directory.
pub struct BlobFiles {
	dir: PathBuf,
	timestamp_size: usize,
	open: HashMap<u64, (File, u64)>,
}

impl BlobFiles {
	pub(crate) fn new(dir: PathBuf, timestamp_size: usize) -> Self {
		Self {
			dir,
			timestamp_size,
			open: HashMap::new(),
		}
	}

	/// The value a blob index points to, for the key `user_key`.
	///
	/// The record is read whole and checked as RocksDB checks it: its header and its key and value
	/// against their checksums, and its sizes against the index. The key stored with a value
	/// carries the timestamp it was written with; garbage collection can later rewrite the entry
	/// with another, which RocksDB itself reports as a key mismatch, so only the user key part is
	/// compared.
	pub fn read(&mut self, blob: &BlobRef, user_key: &[u8]) -> Result<Vec<u8>> {
		if !self.open.contains_key(&blob.file_number) {
			if self.open.len() >= MAX_OPEN_FILES {
				self.open.clear();
			}
			let path = self.dir.join(format!("{:06}.blob", blob.file_number));
			let mut file = File::open(&path).map_err(|e| {
				corrupt(format!("blob file {} cannot be opened: {e}", path.display()))
			})?;
			let size = file.metadata()?.len();
			let mut header = [0u8; HEADER_SIZE as usize];
			file.read_exact(&mut header)?;
			let mut pos = 0;
			if fixed32(&header, &mut pos)? != BLOB_MAGIC {
				return Err(corrupt(format!("{} is not a blob file", path.display())));
			}
			self.open.insert(blob.file_number, (file, size));
		}
		let (file, file_size) = self.open.get_mut(&blob.file_number).expect("opened above");

		let key_size = (user_key.len() + self.timestamp_size) as u64;
		let adjustment = key_size + RECORD_HEADER_SIZE as u64;
		// The value has to lie after the file header and its record header, and end before the
		// footer a closed file has; RocksDB refuses an offset that does not.
		if blob.offset < HEADER_SIZE + adjustment
			|| blob.offset.checked_add(blob.size).is_none_or(|end| end > *file_size)
		{
			return Err(corrupt(format!(
				"blob {} at {} ({} bytes) lies outside its file of {file_size} bytes",
				blob.file_number, blob.offset, blob.size
			)));
		}
		file.seek(SeekFrom::Start(blob.offset - adjustment))?;
		let mut record = vec![0u8; (adjustment + blob.size) as usize];
		file.read_exact(&mut record).map_err(|e| {
			corrupt(format!(
				"blob {} at {} ({} bytes) is cut short: {e}",
				blob.file_number, blob.offset, blob.size
			))
		})?;

		let mut pos = 0;
		let stored_key_size = fixed64(&record, &mut pos)?;
		let value_size = fixed64(&record, &mut pos)?;
		let _expiration = fixed64(&record, &mut pos)?;
		let header_crc = fixed32(&record, &mut pos)?;
		let blob_crc = fixed32(&record, &mut pos)?;
		if mask_crc(crc32c(&record[..RECORD_HEADER_SIZE - 8])) != header_crc {
			return Err(corrupt(format!(
				"the header of blob {} at {} fails its checksum",
				blob.file_number, blob.offset
			)));
		}
		if stored_key_size != key_size || value_size != blob.size {
			return Err(corrupt(format!(
				"blob {} at {} says it holds a key of {stored_key_size} bytes and a value of {value_size}, the index says {key_size} and {}",
				blob.file_number, blob.offset, blob.size
			)));
		}
		let key = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + key_size as usize];
		let stored = &record[RECORD_HEADER_SIZE + key_size as usize..];
		if mask_crc(crc32c_extend(crc32c(key), stored)) != blob_crc {
			return Err(corrupt(format!(
				"blob {} at {} fails its checksum",
				blob.file_number, blob.offset
			)));
		}
		if &key[..user_key.len()] != user_key {
			return Err(corrupt(format!(
				"blob {} at {} belongs to another key",
				blob.file_number, blob.offset
			)));
		}
		compression::decompress(blob.compression, stored)
	}
}
