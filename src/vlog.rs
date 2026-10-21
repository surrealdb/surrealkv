use std::collections::{BTreeSet, HashMap};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crc32fast::Hasher;
use parking_lot::{Mutex, RwLock, RwLockWriteGuard};

use crate::error::{BackgroundErrorHandler, BackgroundErrorReason, EncryptionError, Error, Result};
use crate::tde::{EncryptionContext, FileCipher};
use crate::vfs::{CreateOpts, Fs, FsFile, FsKind};
use crate::{vfs, CompressionType, Options, VLogChecksumLevel, Value};

pub(crate) mod sealed;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod fs_tests;

/// Test-only: the VLog files whose fsync completed, in order.
#[cfg(test)]
pub(crate) static SYNCED_VLOG_FILES: parking_lot::Mutex<Vec<PathBuf>> =
	parking_lot::Mutex::new(Vec::new());

/// Test-only: for every fsync of a VLog file that completed, the file and the length it had just
/// before the fsync, which is how much of it the fsync is sure to have made durable.
#[cfg(test)]
pub(crate) static SYNCED_VLOG_LENGTHS: parking_lot::Mutex<Vec<(PathBuf, u64)>> =
	parking_lot::Mutex::new(Vec::new());

/// VLog format version
pub const VLOG_FORMAT_VERSION: u16 = 1;

/// Size of a ValuePointer when encoded (1 + 4 + 8 + 4 + 4 + 4 bytes)
pub const VALUE_POINTER_SIZE: usize = 25;

/// Bit flags for meta operations
pub const BIT_VALUE_POINTER: u8 = 1; // Set if the value is NOT stored directly next to key
/// Set if an 8-byte big-endian timestamp follows the version byte. The timestamp is the
/// version's own time (a commit time, or one the caller chose with `set_at`), kept with the value
/// so that the internal key stays flat. Absent from entries written without one.
pub const BIT_HAS_TIMESTAMP: u8 = 2;
pub const VALUE_LOCATION_VERSION: u8 = 1;
/// Size of the timestamp that follows the version byte when `BIT_HAS_TIMESTAMP` is set.
pub(crate) const VALUE_TIMESTAMP_SIZE: usize = 8;
pub const VALUE_POINTER_VERSION: u8 = 1;

/// VLog file header with comprehensive metadata
#[derive(Debug, Clone)]
pub(crate) struct VLogFileHeader {
	pub magic: u32,         // "VLOG"
	pub version: u16,       // Format version
	pub file_id: u32,       // File ID
	pub created_at: u64,    // Creation timestamp
	pub max_file_size: u64, // Maximum file size from options
	pub compression: u8,    // Compression type
	pub reserved: u32,      // Reserved for future use
}

impl VLogFileHeader {
	const MAGIC: u32 = 0x564C_4F47;
	// "VLOG" in hex
	const SIZE: usize = 31;

	// 4 + 2 + 4 + 8 + 8 + 1 + 4 = 31 bytes

	pub(crate) fn new(file_id: u32, max_file_size: u64, compression: u8) -> Self {
		Self {
			magic: Self::MAGIC,
			version: VLOG_FORMAT_VERSION,
			file_id,
			created_at: crate::rt::now_unix_ns() / 1_000_000,
			max_file_size,
			compression,
			reserved: 0,
		}
	}

	pub(crate) fn encode(&self) -> [u8; Self::SIZE] {
		let mut encoded = [0u8; Self::SIZE];
		let mut offset = 0;

		// Magic (4 bytes)
		encoded[offset..offset + 4].copy_from_slice(&self.magic.to_be_bytes());
		offset += 4;

		// Version (2 bytes)
		encoded[offset..offset + 2].copy_from_slice(&self.version.to_be_bytes());
		offset += 2;

		// File ID (4 bytes)
		encoded[offset..offset + 4].copy_from_slice(&self.file_id.to_be_bytes());
		offset += 4;

		// Created at (8 bytes)
		encoded[offset..offset + 8].copy_from_slice(&self.created_at.to_be_bytes());
		offset += 8;

		// Max file size (8 bytes)
		encoded[offset..offset + 8].copy_from_slice(&self.max_file_size.to_be_bytes());
		offset += 8;

		// Compression (1 byte)
		encoded[offset] = self.compression;
		offset += 1;

		// Reserved (4 bytes)
		encoded[offset..offset + 4].copy_from_slice(&self.reserved.to_be_bytes());

		encoded
	}

	pub(crate) fn decode(data: &[u8]) -> Result<Self> {
		if data.len() != Self::SIZE {
			return Err(Error::Corruption(format!(
				"Invalid VLog header size: expected {} bytes, got {} bytes",
				Self::SIZE,
				data.len()
			)));
		}

		let mut offset = 0;

		// Magic (4 bytes)
		let magic = u32::from_be_bytes([
			data[offset],
			data[offset + 1],
			data[offset + 2],
			data[offset + 3],
		]);
		offset += 4;

		if magic != Self::MAGIC {
			return Err(Error::Corruption(format!(
				"Invalid VLog magic number: expected 0x{:08X}, got 0x{:08X}",
				Self::MAGIC,
				magic
			)));
		}

		// Version (2 bytes)
		let version = u16::from_be_bytes([data[offset], data[offset + 1]]);
		offset += 2;

		// File ID (4 bytes)
		let file_id = u32::from_be_bytes([
			data[offset],
			data[offset + 1],
			data[offset + 2],
			data[offset + 3],
		]);
		offset += 4;

		// Created at (8 bytes)
		let created_at = u64::from_be_bytes([
			data[offset],
			data[offset + 1],
			data[offset + 2],
			data[offset + 3],
			data[offset + 4],
			data[offset + 5],
			data[offset + 6],
			data[offset + 7],
		]);
		offset += 8;

		// Max file size (8 bytes)
		let max_file_size = u64::from_be_bytes([
			data[offset],
			data[offset + 1],
			data[offset + 2],
			data[offset + 3],
			data[offset + 4],
			data[offset + 5],
			data[offset + 6],
			data[offset + 7],
		]);
		offset += 8;

		// Compression (1 byte)
		let compression = data[offset];
		offset += 1;

		// Reserved (4 bytes)
		let reserved = u32::from_be_bytes([
			data[offset],
			data[offset + 1],
			data[offset + 2],
			data[offset + 3],
		]);

		Ok(Self {
			magic,
			version,
			file_id,
			created_at,
			max_file_size,
			compression,
			reserved,
		})
	}

	pub(crate) fn is_compatible(&self) -> bool {
		self.version == VLOG_FORMAT_VERSION
	}

	/// Whether `file` is what a crash leaves of plaintext file `file_id` while its header was
	/// being written: zeros where the header did not reach the disk, or the start of the header
	/// and no more. The header is written on its own before any entry, so such a file holds no
	/// entry. An empty file is not torn: it is given its header when it is reopened.
	pub(crate) fn is_torn(file: &[u8], file_id: u32) -> bool {
		if file.is_empty() || file.len() > Self::SIZE {
			return false;
		}
		if file.iter().all(|&byte| byte == 0) {
			return true;
		}
		if file.len() == Self::SIZE {
			return false;
		}
		// What every header of this file starts with: the magic, the version and the file id.
		let mut start = [0u8; 10];
		start[..4].copy_from_slice(&Self::MAGIC.to_be_bytes());
		start[4..6].copy_from_slice(&VLOG_FORMAT_VERSION.to_be_bytes());
		start[6..].copy_from_slice(&file_id.to_be_bytes());
		let len = file.len().min(start.len());
		file[..len] == start[..len]
	}

	pub(crate) fn validate(&self, file_id: u32) -> Result<()> {
		if self.file_id != file_id {
			return Err(Error::Corruption(format!(
				"File ID mismatch: expected {}, got {}",
				file_id, self.file_id
			)));
		}

		Ok(())
	}
}
/// ValueLocation represents the value info that can be associated with a key,
/// including the internal Meta field for various flags and operations.
///
/// Encoded layout: `meta (1) | version (1) | timestamp (8, only if BIT_HAS_TIMESTAMP) | value`.
/// Every reader of encoded bytes goes through this type (`decode`, `peek_pointer_payload`,
/// `peek_timestamp`) so that the optional timestamp never shifts a payload offset by accident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValueLocation {
	/// Meta flags for various operations (value pointer etc.). `BIT_HAS_TIMESTAMP` is never
	/// kept here: it is implied by `timestamp` and set on encode.
	pub meta: u8,
	/// The actual value data
	pub value: Value,
	/// Version number for the value
	pub version: u8,
	/// The version's own timestamp, if it was stored with one
	pub timestamp: Option<u64>,
}

/// Zero-copy view of an encoded [`ValueLocation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ValueLocationRef<'a> {
	pub meta: u8,
	pub value: &'a [u8],
	pub version: u8,
	pub timestamp: Option<u64>,
}

impl<'a> ValueLocationRef<'a> {
	#[inline]
	pub(crate) fn is_value_pointer(&self) -> bool {
		(self.meta & BIT_VALUE_POINTER) != 0
	}

	pub(crate) fn to_owned(self) -> ValueLocation {
		ValueLocation::new(self.meta, self.value.to_vec(), self.version)
			.with_timestamp(self.timestamp)
	}
}

impl ValueLocation {
	/// Creates a new ValueLocation
	pub(crate) fn new(meta: u8, value: Value, version: u8) -> Self {
		Self {
			meta: meta & !BIT_HAS_TIMESTAMP,
			value,
			version,
			timestamp: None,
		}
	}

	/// Sets the timestamp stored with the value.
	pub(crate) fn with_timestamp(mut self, timestamp: Option<u64>) -> Self {
		self.timestamp = timestamp;
		self
	}

	/// Length of the header of an encoded ValueLocation with this meta byte: everything before
	/// the value.
	fn header_len(meta: u8) -> usize {
		if meta & BIT_HAS_TIMESTAMP != 0 {
			2 + VALUE_TIMESTAMP_SIZE
		} else {
			2
		}
	}

	/// The time a version was written at, from the encoded value of the entry of a key of kind
	/// `kind`: the timestamp stored with it, or `None` for what has none. A version written
	/// while the tree was not versioned has none, and neither has a hard delete, which is a
	/// barrier and not a version. The value of a range delete is its end key, not an encoded
	/// value, so it is never read for one.
	pub(crate) fn entry_time(kind: crate::InternalKeyKind, encoded_value: &[u8]) -> Option<u64> {
		if matches!(kind, crate::InternalKeyKind::RangeDelete | crate::InternalKeyKind::Delete) {
			return None;
		}
		Self::peek_timestamp(encoded_value)
	}

	/// The times of the versions of a key, which are given newest write first as the time each was
	/// stamped with, if it was, and whether it is a barrier (a hard delete or a replace).
	///
	/// A version that has no time was written while the tree was not versioned, between versions
	/// that have: it reads as written at the newest time of the versions written before it, so as
	/// after them and at the same time as the latest of them. A tree that was versioned, then not,
	/// then versioned again, reads what it was written without versioning as the newest writes,
	/// and a database that never was reads it as written at 0. Nothing below a barrier counts.
	pub(crate) fn infer_times(versions: &[(Option<u64>, bool)]) -> Vec<u64> {
		let mut times = Vec::new();
		Self::infer_times_into(versions.len(), |idx| versions[idx], &mut times);
		times
	}

	/// As `infer_times`, for `count` versions given by `version`, into `times`, whose capacity is
	/// reused.
	pub(crate) fn infer_times_into(
		count: usize,
		version: impl Fn(usize) -> (Option<u64>, bool),
		times: &mut Vec<u64>,
	) {
		times.clear();
		times.resize(count, 0);
		let mut newest = 0u64;
		for idx in (0..count).rev() {
			let (stamp, barrier) = version(idx);
			if barrier {
				newest = 0;
			}
			times[idx] = match stamp {
				Some(time) => {
					newest = newest.max(time);
					time
				}
				None => newest,
			};
		}
	}

	/// Returns the timestamp of an ENCODED ValueLocation, if it carries one.
	pub(crate) fn peek_timestamp(data: &[u8]) -> Option<u64> {
		match data.first() {
			Some(meta) if meta & BIT_HAS_TIMESTAMP != 0 => {
				let bytes = data.get(2..2 + VALUE_TIMESTAMP_SIZE)?;
				Some(u64::from_be_bytes(bytes.try_into().ok()?))
			}
			_ => None,
		}
	}

	/// Creates a ValueLocation that points to a value in VLog
	pub(crate) fn with_pointer(pointer: ValuePointer) -> Self {
		let encoded_pointer = pointer.encode();
		Self::new(BIT_VALUE_POINTER, encoded_pointer, VALUE_LOCATION_VERSION)
	}

	/// Creates a ValueLocation with inline value
	pub(crate) fn with_inline_value(value: Value) -> Self {
		Self::new(0, value, VALUE_LOCATION_VERSION)
	}

	/// Checks if the value is a pointer to VLog
	pub fn is_value_pointer(&self) -> bool {
		(self.meta & BIT_VALUE_POINTER) != 0
	}

	/// Returns the pointer payload of an ENCODED ValueLocation when its meta
	/// byte has the value-pointer bit set, without copying the value bytes.
	/// Layout: meta (1 byte), version (1 byte), timestamp (8 bytes, if the meta byte says so),
	/// value. The payload starts after the timestamp, so a pointer is found whether or not the
	/// entry carries one.
	pub(crate) fn peek_pointer_payload(data: &[u8]) -> Option<&[u8]> {
		match data.first() {
			Some(&meta) if meta & BIT_VALUE_POINTER != 0 => data.get(Self::header_len(meta)..),
			_ => None,
		}
	}

	/// Calculates the encoded size of this ValueLocation
	pub(crate) fn encoded_size(&self) -> usize {
		// meta (1 byte) + version (1 byte) + optional timestamp + value length
		let timestamp = if self.timestamp.is_some() {
			VALUE_TIMESTAMP_SIZE
		} else {
			0
		};
		1 + 1 + timestamp + self.value.len()
	}

	/// Encodes the ValueLocation into a byte vector
	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut encoded = Vec::with_capacity(self.encoded_size());
		self.encode_into(&mut encoded).unwrap();
		encoded
	}

	/// Encodes the ValueLocation into a writer
	pub(crate) fn encode_into<W: Write>(&self, writer: &mut W) -> Result<()> {
		// Write meta byte, with the timestamp bit set exactly when a timestamp follows
		let meta = if self.timestamp.is_some() {
			self.meta | BIT_HAS_TIMESTAMP
		} else {
			self.meta & !BIT_HAS_TIMESTAMP
		};
		writer.write_all(&[meta])?;

		// Write version as u8
		writer.write_all(&[self.version])?;

		// Write the timestamp
		if let Some(timestamp) = self.timestamp {
			writer.write_all(&timestamp.to_be_bytes())?;
		}

		// Write value
		writer.write_all(&self.value)?;

		Ok(())
	}

	/// Decodes a [`ValueLocationRef`] from bytes without copying the payload.
	pub(crate) fn decode_ref(data: &[u8]) -> Result<ValueLocationRef<'_>> {
		if data.len() < 2 {
			return Err(Error::Io(Arc::new(io::Error::new(
				io::ErrorKind::UnexpectedEof,
				"failed to fill whole buffer",
			))));
		}
		let meta = data[0];
		let version = data[1];
		let (timestamp, header_len) = if meta & BIT_HAS_TIMESTAMP != 0 {
			let bytes = data.get(2..2 + VALUE_TIMESTAMP_SIZE).ok_or_else(|| {
				Error::Corruption(
					"a value location says it has a timestamp but is too short to hold it"
						.to_string(),
				)
			})?;
			(Some(u64::from_be_bytes(bytes.try_into().unwrap())), 2 + VALUE_TIMESTAMP_SIZE)
		} else {
			(None, 2)
		};

		Ok(ValueLocationRef {
			meta: meta & !BIT_HAS_TIMESTAMP,
			value: &data[header_len..],
			version,
			timestamp,
		})
	}

	/// Decodes a ValueLocation from bytes
	pub(crate) fn decode(data: &[u8]) -> Result<Self> {
		Self::decode_ref(data).map(ValueLocationRef::to_owned)
	}

	/// Resolves an encoded `ValueLocation` slice, borrowing inline value bytes directly
	/// without allocation or fetching from `vlog` when the entry is a pointer.
	pub(crate) fn resolve_raw_value<'a>(
		data: &'a [u8],
		vlog: Option<&Arc<VLog>>,
	) -> Result<std::borrow::Cow<'a, [u8]>> {
		let loc = Self::decode_ref(data)?;
		if loc.is_value_pointer() {
			if let Some(vlog) = vlog {
				let pointer = ValuePointer::decode(loc.value)?;
				let resolved = vlog.get(&pointer).map_err(|e| match e {
					Error::Encryption(_) | Error::FilePoolExhausted => e,
					e => Error::Other(format!(
						"Failed to resolve value from VLog: {e}. ValuePointer: {:?}",
						pointer
					)),
				})?;
				Ok(std::borrow::Cow::Owned(resolved))
			} else {
				Err(Error::Other("VLog not available for pointer resolution".to_string()))
			}
		} else {
			Ok(std::borrow::Cow::Borrowed(loc.value))
		}
	}

	/// Resolves an owned encoded `ValueLocation` buffer, stripping the header in-place
	/// for inline values with zero additional allocations.
	pub(crate) fn resolve_owned_value(mut data: Value, vlog: Option<&Arc<VLog>>) -> Result<Value> {
		let loc = Self::decode_ref(&data)?;
		if loc.is_value_pointer() {
			if let Some(vlog) = vlog {
				let pointer = ValuePointer::decode(loc.value)?;
				vlog.get(&pointer).map_err(|e| match e {
					Error::Encryption(_) | Error::FilePoolExhausted => e,
					e => Error::Other(format!(
						"Failed to resolve value from VLog: {e}. ValuePointer: {:?}",
						pointer
					)),
				})
			} else {
				Err(Error::Other("VLog not available for pointer resolution".to_string()))
			}
		} else {
			let header_len = Self::header_len(data[0]);
			data.drain(..header_len);
			Ok(data)
		}
	}

	/// Resolves an encoded `ValueLocation` slice into `out`, reusing `out`'s capacity.
	pub(crate) fn resolve_value_into(
		data: &[u8],
		vlog: Option<&Arc<VLog>>,
		out: &mut Vec<u8>,
	) -> Result<()> {
		let loc = Self::decode_ref(data)?;
		if loc.is_value_pointer() {
			if let Some(vlog) = vlog {
				let pointer = ValuePointer::decode(loc.value)?;
				vlog.get_into(&pointer, out).map_err(|e| match e {
					Error::Encryption(_) | Error::FilePoolExhausted => e,
					e => Error::Other(format!(
						"Failed to resolve value from VLog: {e}. ValuePointer: {:?}",
						pointer
					)),
				})
			} else {
				Err(Error::Other("VLog not available for pointer resolution".to_string()))
			}
		} else {
			out.clear();
			out.extend_from_slice(loc.value);
			Ok(())
		}
	}

	/// Resolves the actual value, returning inline bytes directly or fetching from VLog.
	#[cfg(test)]
	pub(crate) fn resolve_value(self, vlog: Option<&Arc<VLog>>) -> Result<Value> {
		if self.is_value_pointer() {
			if let Some(vlog) = vlog {
				let pointer = ValuePointer::decode(&self.value)?;
				vlog.get(&pointer).map_err(|e| match e {
					// A failed authentication is not a missing value: the caller tells them
					// apart, so the error is not turned into text.
					Error::Encryption(_) | Error::FilePoolExhausted => e,
					e => Error::Other(format!(
						"Failed to resolve value from VLog: {e}. ValuePointer: {:?}",
						pointer
					)),
				})
			} else {
				Err(Error::Other("VLog not available for pointer resolution".to_string()))
			}
		} else {
			// Value is stored inline
			Ok(self.value)
		}
	}
}

/// A pointer to a value stored in the value log
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValuePointer {
	/// Version of the ValuePointer format
	pub version: u8,
	/// ID of the VLog file containing the value
	pub file_id: u32,
	/// Offset within the file where the value starts
	pub offset: u64,
	/// Size of the key in bytes
	pub key_size: u32,
	/// Size of the value in bytes
	pub value_size: u32,
	/// CRC32 checksum of the value for integrity verification
	pub checksum: u32,
}

/// The version of a pointer to an entry that is sealed: its key and value are one AEAD message
/// in the file, and it is longer than the entry of a plaintext log by the length of the key
/// and the tag. A pointer is still the same 25 bytes.
pub const VALUE_POINTER_SEALED_VERSION: u8 = 2;

impl ValuePointer {
	/// Creates a new ValuePointer
	pub(crate) fn new(
		file_id: u32,
		offset: u64,
		key_size: u32,
		value_size: u32,
		checksum: u32,
	) -> Self {
		Self {
			version: VALUE_POINTER_VERSION,
			file_id,
			offset,
			key_size,
			value_size,
			checksum,
		}
	}

	/// Creates a pointer to a sealed entry. `checksum` is the CRC-32 of the entry as it is stored,
	/// ciphertext and tag included, and the sizes are those of the key and the value.
	pub(crate) fn sealed(
		file_id: u32,
		offset: u64,
		key_size: u32,
		value_size: u32,
		checksum: u32,
	) -> Self {
		Self {
			version: VALUE_POINTER_SEALED_VERSION,
			..Self::new(file_id, offset, key_size, value_size, checksum)
		}
	}

	/// Whether the pointer is to a sealed entry, see [`VALUE_POINTER_SEALED_VERSION`].
	pub(crate) fn is_sealed(&self) -> bool {
		self.version == VALUE_POINTER_SEALED_VERSION
	}

	/// Encodes the pointer as bytes for storage
	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut encoded = Vec::with_capacity(VALUE_POINTER_SIZE);
		encoded.push(self.version);
		encoded.extend_from_slice(&self.file_id.to_be_bytes());
		encoded.extend_from_slice(&self.offset.to_be_bytes());
		encoded.extend_from_slice(&self.key_size.to_be_bytes());
		encoded.extend_from_slice(&self.value_size.to_be_bytes());
		encoded.extend_from_slice(&self.checksum.to_be_bytes());
		encoded
	}

	/// Decodes a pointer from bytes
	pub(crate) fn decode(data: &[u8]) -> Result<Self> {
		if data.len() != VALUE_POINTER_SIZE {
			return Err(Error::Corruption(format!(
				"Invalid ValuePointer size: expected {} bytes, got {} bytes",
				VALUE_POINTER_SIZE,
				data.len()
			)));
		}

		let version = data[0];
		let file_id = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
		let offset = u64::from_be_bytes([
			data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[12],
		]);
		let key_size = u32::from_be_bytes([data[13], data[14], data[15], data[16]]);
		let value_size = u32::from_be_bytes([data[17], data[18], data[19], data[20]]);
		let checksum = u32::from_be_bytes([data[21], data[22], data[23], data[24]]);

		Ok(Self {
			version,
			file_id,
			offset,
			key_size,
			value_size,
			checksum,
		})
	}

	/// The size of the entry in its file. A plaintext entry is a header with both lengths, the
	/// key, the value and a crc32. A sealed one has no header, as nothing about it is in the
	/// clear: its length is what the pointer says, and the length of the key, the key, the
	/// value and the tag are one message.
	pub(crate) fn total_entry_size(&self) -> u64 {
		let content = self.key_size as u64 + self.value_size as u64;
		if self.is_sealed() {
			sealed::ENTRY_OVERHEAD as u64 + content
		} else {
			8 + content + 4
		}
	}
}

/// A file of the log that is open for reading: the handle, and for a sealed file the cipher that
/// opens its entries, which is made from its header once, when the file is first opened, and
/// goes when the handle does.
///
/// The handle of the file that is being appended to is the one the writer appends through (see
/// [`VLogWriter`]): one handle per file serves both, so that a file system that allows one open
/// handle per path (a browser's) is never asked for a second.
pub(crate) struct OpenFile {
	file: Arc<dyn FsFile>,
	cipher: Option<FileCipher>,
}

/// Represents a VLog file that can be read from
#[derive(Debug)]
pub(crate) struct VLogFile {
	/// File path
	pub path: PathBuf,
}

impl VLogFile {
	fn new(_id: u32, path: PathBuf, _size: u64) -> Self {
		Self {
			path,
		}
	}
}

/// The file a [`VLogWriter`] writes to. In tests a write can be made to fail, as a full disk
/// does.
struct Sink {
	file: Arc<dyn FsFile>,
	/// Where the next byte lands: the end of the file when the sink was made, and every byte that
	/// has been appended since. It is only read when an append fails, to tell how much of it
	/// landed, so that a short write is reported as one and the buffer in front of the sink drops
	/// what is in the file and keeps the rest.
	end: u64,
	/// Bytes the file still takes before one write fails.
	#[cfg(test)]
	fail_after: Option<usize>,
}

impl Sink {
	/// A sink that appends to `file`, whose end is `end`.
	fn new(file: Arc<dyn FsFile>, end: u64) -> Self {
		Self {
			file,
			end,
			#[cfg(test)]
			fail_after: None,
		}
	}

	/// Appends `buf` to the file. An append is all or nothing to the caller of `FsFile::append`,
	/// and a write that fails after it wrote a part is what `Write::write` calls a short write:
	/// the part that landed is reported, and the next call fails with nothing landed, as a
	/// write to a full disk does.
	fn append(&mut self, buf: &[u8]) -> io::Result<usize> {
		match self.file.append(buf) {
			Ok(()) => {
				self.end += buf.len() as u64;
				Ok(buf.len())
			}
			Err(e) => {
				// How much landed is told by the file, and only on this path.
				let landed = match self.file.len() {
					Ok(len) => len.saturating_sub(self.end).min(buf.len() as u64),
					Err(_) => 0,
				};
				if landed == 0 {
					return Err(e);
				}
				self.end += landed;
				Ok(landed as usize)
			}
		}
	}
}

impl Write for Sink {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		#[cfg(test)]
		if let Some(left) = self.fail_after {
			if left == 0 {
				self.fail_after = None;
				return Err(io::Error::other("injected write failure"));
			}
			let written = self.append(&buf[..buf.len().min(left)])?;
			self.fail_after = Some(left - written);
			return Ok(written);
		}
		self.append(buf)
	}

	fn flush(&mut self) -> io::Result<()> {
		Ok(())
	}
}

/// The lengths of the key and the value of an entry as its header and its pointer store them, a
/// `u32` each. An entry with a longer key or value is refused: a cast would store a length that
/// wrapped around, and the entry could not be read back.
pub(crate) fn entry_lens(key: usize, value: usize) -> Result<(u32, u32)> {
	match (u32::try_from(key), u32::try_from(value)) {
		(Ok(key), Ok(value)) => Ok((key, value)),
		_ => Err(Error::InvalidArgument(format!(
			"a VLog entry takes a key and a value of at most {} bytes each, got a key of {key} \
			 bytes and a value of {value} bytes",
			u32::MAX
		))),
	}
}

/// Writer for a single VLog file
pub(crate) struct VLogWriter {
	/// Buffered writer for the file
	writer: BufWriter<Sink>,
	/// The file the buffer writes to, for fsync outside the write lock, for the length that a cut
	/// needs and for `set_len`. It is the handle of the buffer's sink, shared, and the handle that
	/// the log reads the file through: there is one per file, and none is duplicated.
	file: Arc<dyn FsFile>,
	/// Current offset in the file: its length once everything buffered is written
	pub(crate) current_offset: u64,
	/// ID of this VLog file
	file_id: u32,
	/// Total bytes written to this file
	bytes_written: u64,
	/// Set when an append failed and the file could not be cut back to the end of the last
	/// complete entry, or when an append to a sealed file failed at all. The writer then
	/// refuses appends, and the log replaces it on its next append.
	poisoned: bool,
	/// The cipher that seals the entries of a sealed file, which is the only thing that seals
	/// in it: `None` for a plaintext file.
	cipher: Option<FileCipher>,
	/// Test-only: the next cut of a failed append fails.
	#[cfg(test)]
	fail_cut_back: bool,
}

impl VLogWriter {
	/// Creates a new VLogWriter for the specified file, on the operating system's file system.
	/// What the tests call: the log itself goes through [`new_in`](Self::new_in).
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn new(
		path: &Path,
		file_id: u32,
		max_file_size: u64,
		compression: u8,
	) -> Result<Self> {
		Self::new_in(&*vfs::default_fs(), path, file_id, max_file_size, compression)
	}

	/// Creates a new VLogWriter for the specified file, in `fs`: the file is opened to append, and
	/// created if it is not there.
	pub(crate) fn new_in(
		fs: &dyn Fs,
		path: &Path,
		file_id: u32,
		max_file_size: u64,
		compression: u8,
	) -> Result<Self> {
		let file_exists = fs.exists(path);
		let file = fs.open_append(path, CreateOpts::default())?;
		Self::from_file(file, file_exists, file_id, max_file_size, compression)
	}

	/// The writer of a file that is already open to append and to read, which is the handle that
	/// the writer keeps and shares: a log that has opened its newest file to read it hands the
	/// same handle on, and no second one is opened. `file_exists` is whether the file was there
	/// before it was opened.
	pub(crate) fn from_file(
		file: Arc<dyn FsFile>,
		file_exists: bool,
		file_id: u32,
		max_file_size: u64,
		compression: u8,
	) -> Result<Self> {
		let current_offset = file.len()?;
		if current_offset > 0 {
			// A sealed file is never appended to by a writer that does not seal: its entries
			// would be plaintext in the middle of ciphertext, and the sealing writer of the file
			// is gone, so no other may pick up where it stopped.
			//
			// The handle reads as well as appends, so the start of the file is read through it.
			let mut start = [0u8; 6];
			let read = vfs::File::read_up_to(&*file, 0, &mut start)?;
			if sealed::is_sealed_start(&start[..read]) {
				return Err(EncryptionError::InvalidInput(
					"a sealed value-log file is never reopened for appending",
				)
				.into());
			}
		}
		let mut writer = BufWriter::new(Sink::new(Arc::clone(&file), current_offset));

		// If this is a new file, write the header
		if !file_exists || current_offset == 0 {
			let header = VLogFileHeader::new(file_id, max_file_size, compression);
			let header_bytes = header.encode();
			writer.write_all(&header_bytes)?;
			writer.flush()?;
		}

		// Update offset to account for header
		let current_offset = if !file_exists || current_offset == 0 {
			VLogFileHeader::SIZE as u64
		} else {
			current_offset
		};

		Ok(Self {
			writer,
			file,
			current_offset,
			file_id,
			bytes_written: current_offset,
			poisoned: false,
			cipher: None,
			#[cfg(test)]
			fail_cut_back: false,
		})
	}

	/// Creates the writer of a new sealed file, which is as long as its header and has its
	/// header on disk. The file must not exist: the writer seals under a key that is its own, and
	/// no writer ever picks a file up again. What the tests call: the log itself goes through
	/// [`create_sealed_in`](Self::create_sealed_in).
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn create_sealed(
		path: &Path,
		file_id: u32,
		max_file_size: u64,
		compression: u8,
		context: &EncryptionContext,
	) -> Result<Self> {
		Self::create_sealed_in(
			&*vfs::default_fs(),
			path,
			file_id,
			max_file_size,
			compression,
			context,
		)
	}

	/// As [`create_sealed`](Self::create_sealed), in `fs`. A sealed file is made with `std::fs`
	/// (see `sealed::create`), so it is only made on a file system that is `std`: any other is
	/// refused before a file is created.
	pub(crate) fn create_sealed_in(
		fs: &dyn Fs,
		path: &Path,
		file_id: u32,
		max_file_size: u64,
		compression: u8,
		context: &EncryptionContext,
	) -> Result<Self> {
		refuse_sealed_off_std(fs)?;
		#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
		{
			use crate::vfs::IntoFsFile;

			let (file, cipher) =
				sealed::create(path, file_id, max_file_size, compression, context)?;
			let file = file.into_fs_file();
			let header_len = sealed::SEALED_HEADER_LEN as u64;
			Ok(Self {
				writer: BufWriter::new(Sink::new(Arc::clone(&file), header_len)),
				file,
				current_offset: header_len,
				file_id,
				bytes_written: header_len,
				poisoned: false,
				cipher: Some(cipher),
				#[cfg(test)]
				fail_cut_back: false,
			})
		}
		#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
		{
			let _ = (path, file_id, max_file_size, compression, context);
			Err(sealed_refusal())
		}
	}

	/// Appends a key+value entry to the file and returns a pointer to it
	/// Layout: +--------+-----+-------+-------+
	///         | header  | key | value | crc32 |
	///         +--------+-----+-------+--------+
	///
	/// A failed append leaves the file as it was: the part of the entry that reached the
	/// buffer or the file is cut off, so the next entry starts where its pointer says. If the
	/// cut fails too, the writer is poisoned.
	pub(crate) fn append(&mut self, key: &[u8], value: &[u8]) -> Result<ValuePointer> {
		if self.poisoned {
			return Err(Error::Io(Arc::new(io::Error::other(
				"VLog writer is poisoned by an earlier failed append",
			))));
		}

		let (key_len, value_len) = entry_lens(key.len(), value.len())?;
		if self.cipher.is_some() {
			return self.append_sealed(key, value, (key_len, value_len));
		}
		let offset = self.current_offset;

		// Calculate CRC32 of key + value
		let mut hasher = Hasher::new();
		hasher.update(key);
		hasher.update(value);
		let crc32 = hasher.finalize();

		if let Err(e) = self.write_entry(key, value, (key_len, value_len), crc32) {
			if let Err(cut) = self.cut_back(offset) {
				tracing::error!(
					"Failed to cut a partial entry off VLog file {}, poisoning its writer: {cut}",
					self.file_id
				);
				self.poisoned = true;
			}
			return Err(e);
		}

		let entry_size = 8 + key.len() as u64 + value.len() as u64 + 4; // header + key + value +
																		// crc32
		self.current_offset += entry_size;
		self.bytes_written += entry_size;

		Ok(ValuePointer::new(self.file_id, offset, key_len, value_len, crc32))
	}

	/// Appends an entry to a sealed file.
	///
	/// The entry is sealed at its offset, which is its nonce slot, and a position is sealed once
	/// per key: so the position is spoken for the moment it is given to the cipher, before any
	/// byte is written, and the writer is poisoned until the append has gone through. Any way out
	/// of here but the end, a failed seal or a failed or partial write, leaves it poisoned, and the
	/// log then starts a file of its own, with a key of its own. The file cannot be cut back and
	/// tried again the way a plaintext one is: the position that the failed entry had would be
	/// sealed a second time with other bytes, which under the same key and nonce leaks what the
	/// two entries have in common. What an earlier append left in the buffer is still written.
	fn append_sealed(
		&mut self,
		key: &[u8],
		value: &[u8],
		(key_len, value_len): (u32, u32),
	) -> Result<ValuePointer> {
		let offset = self.current_offset;

		// What can fail without taking the position: the allocation, and an offset that does
		// not fit.
		let mut entry = sealed::frame(key, value)?;
		let stored_len = (entry.len() + crate::tde::TAG_LEN) as u64;
		let next = offset.checked_add(stored_len).ok_or_else(|| {
			Error::InvalidArgument("The offset in a VLog file does not fit in 64 bits".to_string())
		})?;

		// From here the position is spoken for, whatever happens to the entry.
		self.poisoned = true;
		self.current_offset = next;
		self.bytes_written += stored_len;

		let cipher = self.cipher.as_ref().ok_or_else(|| {
			Error::Other("a sealed append was made on a writer without a cipher".to_string())
		})?;
		sealed::seal(cipher, offset, &mut entry)?;
		let checksum = crc32fast::hash(&entry);
		self.writer.write_all(&entry)?;

		self.poisoned = false;
		Ok(ValuePointer::sealed(self.file_id, offset, key_len, value_len, checksum))
	}

	fn write_entry(
		&mut self,
		key: &[u8],
		value: &[u8],
		(key_len, value_len): (u32, u32),
		crc32: u32,
	) -> Result<()> {
		// Write header: [key_len: 4 bytes][value_len: 4 bytes]
		self.writer.write_all(&key_len.to_be_bytes())?;
		self.writer.write_all(&value_len.to_be_bytes())?;

		// Write key
		self.writer.write_all(key)?;

		// Write value
		self.writer.write_all(value)?;

		// Write CRC32
		self.writer.write_all(&crc32.to_be_bytes())?;
		Ok(())
	}

	/// Cuts everything after the first `len` bytes of the file's stream (the file followed by
	/// the buffer) off: the bytes of an entry whose append failed.
	///
	/// Entries before it may still be partly in the buffer, with pointers that were already
	/// handed out, so the buffer is kept up to `len` and not dropped. What is kept goes into a
	/// buffer that has room for it, as a buffer that the bytes fill would write them straight to
	/// the file, where they could fail again. Once the file has been cut, nothing that follows
	/// touches the disk, so a failed cut loses no entry that was appended before.
	fn cut_back(&mut self, len: u64) -> Result<()> {
		#[cfg(test)]
		if std::mem::take(&mut self.fail_cut_back) {
			return Err(Error::Io(Arc::new(io::Error::other("injected cut failure"))));
		}
		let on_disk = self.file.len()?;
		if on_disk > len {
			// The entries before are all in the file, so what is buffered belongs to the failed
			// one.
			self.file.set_len(len)?;
		}
		// The sink picks up at the end of the file as it is now, which the cut has just made.
		let sink = Sink::new(Arc::clone(&self.file), on_disk.min(len));

		// Rebuild the buffer around the same file. The old one is taken apart rather than
		// dropped, because dropping a `BufWriter` writes out what it still holds.
		let kept = usize::try_from(len.saturating_sub(on_disk))
			.map_err(|_| Error::Io(Arc::new(io::Error::other("a VLog buffer was lost"))))?;
		let capacity = self.writer.capacity().max(kept + 1);
		let stale = std::mem::replace(&mut self.writer, BufWriter::with_capacity(capacity, sink));
		let (_, buffered) = stale.into_parts();
		if kept > 0 {
			let lost = || Error::Io(Arc::new(io::Error::other("a VLog buffer was lost")));
			let buffered = buffered.map_err(|_| lost())?;
			self.writer.write_all(buffered.get(..kept).ok_or_else(lost)?)?;
		}
		Ok(())
	}

	/// Whether the next append belongs in another file: this one is full, or its writer is
	/// poisoned.
	fn needs_replacing(&self, max_file_size: u64) -> bool {
		self.poisoned || self.size() >= max_file_size
	}

	/// Test-only: the file takes `bytes` more bytes and then one write fails, as a full disk
	/// makes it. Whatever is buffered reaches the file up to that point, as a short write does.
	#[cfg(test)]
	pub(crate) fn fail_writes_after(&mut self, bytes: usize) {
		self.writer.get_mut().fail_after = Some(bytes);
	}

	/// Test-only: the next cut of a failed append fails, as a file that cannot be truncated
	/// does.
	#[cfg(test)]
	pub(crate) fn fail_next_cut_back(&mut self) {
		self.fail_cut_back = true;
	}

	/// Test-only: the first position that may still be sealed in a sealed file, which is one past
	/// the last that was given to the cipher.
	#[cfg(test)]
	pub(crate) fn next_position_to_seal(&self) -> Option<u64> {
		self.cipher.as_ref().map(|cipher| cipher.next_position())
	}

	/// Test-only: marks the writer as one whose failed append could not be cut back.
	#[cfg(test)]
	pub(crate) fn poison(&mut self) {
		self.poisoned = true;
	}

	/// Flushes and syncs the writer (test helper).
	#[cfg(test)]
	pub(crate) fn sync(&mut self) -> Result<()> {
		self.writer.flush()?;
		self.writer.get_ref().file.sync()?;
		Ok(())
	}

	/// Flushes buffered data to OS cache (not to disk).
	pub(crate) fn flush(&mut self) -> Result<()> {
		self.writer.flush()?;
		Ok(())
	}

	/// Returns the handle of the file, shared, for performing fsync outside the write lock. It is
	/// the one handle the file has: an fsync of it and a write through it at the same time are what
	/// an fsync of a duplicated descriptor and a write through the original were.
	pub(crate) fn sync_fd(&self) -> Arc<dyn FsFile> {
		Arc::clone(&self.file)
	}

	/// Gets the current size of the file
	fn size(&self) -> u64 {
		self.current_offset
	}
}

/// A count of changes that an fsync has to cover, and how many of them an fsync that completed
/// covers.
///
/// An fsync settles what it covers only once it has succeeded. One that is still in flight, or
/// that failed, leaves its changes pending, so a caller that asks meanwhile fsyncs them itself
/// instead of relying on it.
#[derive(Default)]
struct Pending {
	changed: AtomicU64,
	synced: AtomicU64,
}

impl Pending {
	fn mark(&self) {
		self.changed.fetch_add(1, Ordering::SeqCst);
	}

	fn count(&self) -> u64 {
		self.changed.load(Ordering::SeqCst)
	}

	/// Whether an fsync has completed on the first `count` changes.
	fn covers(&self, count: u64) -> bool {
		self.synced.load(Ordering::SeqCst) >= count
	}

	fn settle(&self, count: u64) {
		self.synced.fetch_max(count, Ordering::SeqCst);
	}

	fn is_clean(&self) -> bool {
		self.covers(self.count())
	}
}

/// Value Log (VLog) for WiscKey-style key-value separation with GC
///
/// This implementation includes:
/// - Iterator reference counting for safe garbage collection
/// - Discard statistics tracking for intelligent GC candidate selection
/// - Atomic file replacement during compaction
/// - LSM integration for staleness detection
pub(crate) struct VLog {
	/// The file system the files are in.
	fs: Arc<dyn Fs>,

	/// Base directory for VLog files
	path: PathBuf,

	/// Maximum size for each VLog file before rotation
	max_file_size: u64,

	/// Checksum verification level
	checksum_level: VLogChecksumLevel,

	/// Next file ID to be assigned
	pub(crate) next_file_id: AtomicU32,

	/// ID of the file currently open for writing
	pub(crate) active_writer_id: AtomicU32,

	/// Writer for the current active file
	pub(crate) writer: RwLock<Option<VLogWriter>>,

	/// Files the writer rolled over from, until an fsync of each has succeeded: their bytes
	/// reached the OS but may not be on disk. Locked after `writer`, never before it.
	unsynced: Mutex<Vec<(u32, Arc<dyn FsFile>)>>,

	/// The entries appended, marked under the `writer` lock, and how many of them a completed
	/// fsync covers.
	appended: Pending,

	/// The files created, whose directory entry a completed fsync of the directory covers.
	created: Pending,

	/// Maps file_id to VLogFile metadata
	files_map: RwLock<HashMap<u32, Arc<VLogFile>>>,

	/// Cached open file handles for reading
	pub(crate) file_handles: RwLock<HashMap<u32, Arc<OpenFile>>>,

	/// Replaced, under the `file_handles` lock, by every [`reload`](Self::reload): a handle that
	/// was opened before it belongs to a file that was replaced, and is not cached. It is drawn
	/// from the same counter as the identity of a table instance, so that no other log, nor any
	/// generation of this one, has the same value: logs that are given clones of one `Options`
	/// share a block cache, and the values of one must not be found under the key of another.
	generation: AtomicU64,

	/// Options for VLog configuration
	pub(crate) opts: Arc<Options>,

	/// Set by the first fsync of a file of the log that fails, and cleared only by a
	/// [`reload`](Self::reload), which replaces every file: see [`sync`](Self::sync).
	sync_failed: AtomicBool,

	/// Held across each fsync of a file of the log, with the check of `sync_failed` before it and
	/// the setting of it after: see [`guarded_sync`](Self::guarded_sync).
	sync_serial: Mutex<()>,

	/// Told of the first failed fsync, which stops the database. Set once the tree that owns the
	/// log has its handler, see [`set_error_handler`](Self::set_error_handler).
	error_handler: OnceLock<Arc<BackgroundErrorHandler>>,

	/// Test-only: the next fsync of a file fails.
	#[cfg(test)]
	fail_next_sync: std::sync::atomic::AtomicBool,

	/// Test-only: the calls that reached [`guarded_sync`](Self::guarded_sync), the ones waiting
	/// for `sync_serial` included.
	#[cfg(test)]
	sync_arrivals: AtomicUsize,

	/// Test-only: runs inside every fsync of a file, with `sync_serial` held, before the fsync.
	#[cfg(test)]
	sync_observer: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,

	/// Test-only: runs in `sync` between taking the fds and fsyncing them.
	#[cfg(test)]
	sync_gap: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,

	/// Number of live [`VLogPin`]s. While there is one, no file is deleted.
	pins: AtomicUsize,

	/// The file that the commit group in flight started in, `u32::MAX` while there is none. Set
	/// by [`pin_group`](Self::pin_group), by the one flusher (and by the test-only `flush_group`,
	/// which must not run beside it): a second group at a time would release the pin of the
	/// first.
	group_floor: AtomicU32,
}

/// Keeps a [`VLog`] from deleting files until it is dropped.
pub(crate) struct VLogPin<'a>(&'a VLog);

impl Drop for VLogPin<'_> {
	fn drop(&mut self) {
		self.0.pins.fetch_sub(1, Ordering::SeqCst);
	}
}

/// Keeps a [`VLog`] from deleting the files that the values of a commit group are written to
/// until it is dropped, see [`VLog::pin_group`].
pub(crate) struct VLogGroupPin<'a>(&'a VLog);

impl Drop for VLogGroupPin<'_> {
	fn drop(&mut self) {
		self.0.group_floor.store(u32::MAX, Ordering::SeqCst);
	}
}

/// The newest file of a log, as [`VLog::load_files`] leaves it.
struct Newest {
	id: u32,
	/// Its entries are sealed, or it was set aside as a file that a crash left half made: either
	/// way nothing is ever appended to it.
	sealed: bool,
	/// The handle the log opened it with, which is the one in `file_handles`: open to append for
	/// a plaintext file, whose writer takes it over, and to read for a sealed file. `None` for a
	/// file that was set aside, which is no longer where it was.
	file: Option<Arc<dyn FsFile>>,
}

/// What every sync of the value log fails with once an fsync of it has failed, see `VLog::sync`.
fn sync_poisoned() -> Error {
	Error::Io(Arc::new(io::Error::other("value log is poisoned by an earlier failed fsync")))
}

/// What a sealed value log is refused with where its files cannot be made: on a file system that
/// is not `std`.
fn sealed_refusal() -> Error {
	Error::InvalidArgument(
		"an encrypted value log is not supported on this file system yet".to_string(),
	)
}

fn refuse_sealed_off_std(fs: &dyn Fs) -> Result<()> {
	if fs.kind() == FsKind::Std {
		Ok(())
	} else {
		Err(sealed_refusal())
	}
}

/// An entry of at least this many bytes is checked against the length of its file before a
/// buffer is allocated for it, on a target whose address space is 32 bits (see
/// [`BOUND_ENTRIES`]). The length of an entry is what a pointer says, and a pointer that is
/// damaged must not be able to ask for more memory than the file has, which there is the
/// difference between an error and an abort. Entries below it, which is every entry in practice,
/// are read as they always were, with no call to ask for the length.
const CHECKED_ENTRY_SIZE: u64 = 16 * 1024 * 1024;

/// Whether the size of an entry is bounded by its file before it is allocated. Only on a 32-bit
/// target: a 64-bit one reads as it did before the value log was moved onto the file system seam,
/// and adds no call to a read.
const BOUND_ENTRIES: bool = cfg!(target_pointer_width = "32");

/// Whether the entry at `offset` of `total_size` bytes is cut short by the end of its file, which
/// is `Some(length of the file)` and is only asked when `bounded` and the entry is large. `None`
/// is "read it". `file_len` is not called for any other entry.
fn entry_bound(
	total_size: u64,
	offset: u64,
	bounded: bool,
	file_len: impl FnOnce() -> Result<u64>,
) -> Result<Option<u64>> {
	if !bounded || total_size < CHECKED_ENTRY_SIZE {
		return Ok(None);
	}
	let file_len = file_len()?;
	if offset.checked_add(total_size).is_none_or(|end| end > file_len) {
		return Ok(Some(file_len));
	}
	Ok(None)
}

impl VLog {
	/// The encryption the log's files are sealed with, if it is configured with any.
	fn encryption(&self) -> Option<&EncryptionContext> {
		self.opts.encryption_context().map(|context| &**context)
	}

	/// Returns the file path for a VLog file with the given ID
	pub(crate) fn vlog_file_path(&self, file_id: u32) -> PathBuf {
		self.opts.vlog_file_path(file_id as u64)
	}

	/// Creates a new VLog instance in the file system of the options, which is the operating
	/// system's unless the options say otherwise.
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		Self::new_in(Arc::clone(&opts.fs), opts)
	}

	/// Creates a new VLog instance in `fs`.
	pub(crate) fn new_in(fs: Arc<dyn Fs>, opts: Arc<Options>) -> Result<Self> {
		// A key manager that was never turned into the database's context would leave the
		// log in plaintext without saying so.
		if opts.key_manager.is_some() && opts.encryption_context().is_none() {
			return Err(Error::InvalidArgument(
				"A key manager is configured but the encryption of the database was not resolved: \
				 refusing to write a value log in plaintext"
					.to_string(),
			));
		}
		// A sealed file is made and renamed with `std::fs` (see `sealed`): it is refused on any
		// other file system before anything is created.
		if opts.encryption_context().is_some() {
			refuse_sealed_off_std(&*fs)?;
		}
		let vlog = Self {
			fs,
			path: opts.vlog_dir(),
			max_file_size: opts.vlog_max_file_size,
			checksum_level: opts.vlog_checksum_verification,
			next_file_id: AtomicU32::new(1),
			active_writer_id: AtomicU32::new(0),
			writer: RwLock::new(None),
			unsynced: Mutex::new(Vec::new()),
			appended: Pending::default(),
			created: Pending::default(),
			files_map: RwLock::new(HashMap::new()),
			file_handles: RwLock::new(HashMap::new()),
			generation: AtomicU64::new(crate::cache::next_instance_id()),
			opts,
			sync_failed: AtomicBool::new(false),
			sync_serial: Mutex::new(()),
			error_handler: OnceLock::new(),
			#[cfg(test)]
			fail_next_sync: std::sync::atomic::AtomicBool::new(false),
			#[cfg(test)]
			sync_arrivals: AtomicUsize::new(0),
			#[cfg(test)]
			sync_observer: Mutex::new(None),
			#[cfg(test)]
			sync_gap: Mutex::new(None),
			pins: AtomicUsize::new(0),
			group_floor: AtomicU32::new(u32::MAX),
		};

		// PRE-FILL ALL EXISTING FILE HANDLES ON STARTUP
		vlog.prefill_file_handles()?;

		Ok(vlog)
	}

	/// Appends a key+value pair to the log and returns a ValuePointer
	pub(crate) fn append(&self, key: &[u8], value: &[u8]) -> Result<ValuePointer> {
		let mut writer = self.writer.write();
		if writer.as_ref().is_none_or(|w| w.needs_replacing(self.max_file_size)) {
			self.roll_over(&mut writer)?;
		}
		let pointer = writer.as_mut().expect("a writer was just installed").append(key, value)?;
		self.appended.mark();
		Ok(pointer)
	}

	/// Replaces the active writer with one for a new file.
	///
	/// The old file is flushed first, and a failure of that flush is an error: dropping its
	/// writer would swallow it and lose entries that pointers were already handed out for. It
	/// is not fsynced here, which would stall every commit behind it, but queued for the next
	/// [`sync`](Self::sync).
	fn roll_over(&self, active: &mut Option<VLogWriter>) -> Result<()> {
		let old = match active.as_mut() {
			Some(writer) => {
				writer.flush()?;
				Some((writer.file_id, writer.sync_fd()))
			}
			None => None,
		};

		// Create new file
		let file_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
		let file_path = self.vlog_file_path(file_id);
		let new_writer = match self.encryption() {
			Some(context) => {
				// A sealed file starts with a new salt and a header that is on disk, and its name
				// is on disk too before anything is written to it: a restart that found entries
				// in a file whose name was lost would not find the file the pointers name.
				let writer = VLogWriter::create_sealed_in(
					&*self.fs,
					&file_path,
					file_id,
					self.opts.vlog_max_file_size,
					CompressionType::None as u8,
					context,
				)?;
				self.sync_dir()?;
				writer
			}
			None => VLogWriter::new_in(
				&*self.fs,
				&file_path,
				file_id,
				self.opts.vlog_max_file_size,
				CompressionType::None as u8,
			)?,
		};

		// Register the new file for GC safety
		self.register_vlog_file(file_id, file_path, 0); // Start with size 0

		// And for reading, through the handle the writer appends through. A file that is read
		// through a handle of its own is opened while the writer holds it, which a file system
		// that gives out one handle per path refuses, and it could not be opened from `get` at
		// all there. A sealed file's handle cannot read (it is made to append only), and the file
		// is opened to read when it is first read, as it always was.
		if self.encryption().is_none() {
			let handle = Arc::new(OpenFile {
				file: new_writer.sync_fd(),
				cipher: None,
			});
			self.file_handles.write().insert(file_id, handle);
		}

		// Update the active writer ID
		self.active_writer_id.store(file_id, Ordering::SeqCst);

		self.unsynced.lock().extend(old);
		self.created.mark();
		if self.encryption().is_some() {
			// The directory was fsynced above, and that covers every file created before it.
			self.created.settle(self.created.count());
		}
		*active = Some(new_writer);
		Ok(())
	}

	/// Pre-fills the file_handles cache with all existing VLog files
	fn prefill_file_handles(&self) -> Result<()> {
		let Some(newest) = self.load_files()? else {
			return Ok(());
		};

		// Found existing files, set next_file_id to one past the highest existing file ID
		self.next_file_id.store(newest.id + 1, Ordering::SeqCst);

		if newest.sealed {
			// A sealed file is never appended to again. Its key is the one of the writer that
			// made it, and the positions that writer sealed at are spoken for, so a writer that
			// picked the file up would start sealing over them again. The first append starts a
			// file of its own, with a salt of its own, and until then the newest file is only
			// read.
			self.active_writer_id.store(newest.id, Ordering::SeqCst);
			if let Some(file) = newest.file {
				// What an earlier run left in the file may not be on disk yet, and there is no
				// writer to hold a handle for the first sync to use: the handle the file was
				// opened with, which the log reads it through too, is the one.
				self.unsynced.lock().push((newest.id, file));
			}
			self.appended.mark();
			return Ok(());
		}

		// Set the last file up as the active writer, through the handle it was opened with
		let file = newest.file.ok_or_else(|| {
			Error::Other(format!("the newest VLog file {} was found without a handle", newest.id))
		})?;
		let writer = VLogWriter::from_file(
			file,
			true,
			newest.id,
			self.opts.vlog_max_file_size,
			CompressionType::None as u8,
		)?;
		self.active_writer_id.store(newest.id, Ordering::SeqCst);
		*self.writer.write() = Some(writer);
		// What an earlier run left in the file may not be on disk yet: the first sync
		// covers it.
		self.appended.mark();

		Ok(())
	}

	/// Opens and validates every file in the log's directory, registers it in `file_handles` and
	/// `files_map`, and returns the one with the highest id. A directory that does not exist
	/// holds none.
	///
	/// A log may find a crash's leftover in a file whose header did not reach the disk: one that
	/// the crash cut off before its header was complete, or whose header is all zeros, and which
	/// therefore holds no entry that is durable. An fsync of a file writes its header with its
	/// entries, and a file is synced before a table, or a segment of the log that another
	/// follows, names an entry of it. Such a file is set aside under another name, never
	/// deleted. If it is the newest file, it is reported as one that is never appended to and is
	/// no longer there, so that its id is not used again: the id of any other file is below the
	/// newest, and is not used again either. Only a header that is no longer than a header and is
	/// incomplete, or that is all zeros, counts: whatever else fails to authenticate is an error
	/// and leaves every file as it is. The header of a file of an encrypted log is made durable
	/// when the file is made, so only its newest file can have been cut off in its header, and a
	/// header of zeros is never taken for a leftover there.
	/// The files are renamed only after every other file has opened, so a log that fails on a key
	/// that does not fit changes nothing, whichever order the directory lists the files in.
	fn load_files(&self) -> Result<Option<Newest>> {
		// A directory that is not there holds none. Any other failure to list it is a failure to
		// open the log: a log that took an unreadable directory for an empty one would number its
		// first file from 1 again, beside the files that it could not see.
		let entries = match self.fs.read_dir(&self.path) {
			Ok(entries) => entries,
			Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
			Err(e) => return Err(e.into()),
		};

		// Which file is the newest has to be known before any is opened
		let mut found = Vec::new();
		for entry in entries {
			// Look for VLog files, by name and not by type, as it always was. The length is the
			// length of the file that the path leads to: where the listing took it from the entry
			// (`DirEntry::metadata`, which does not follow a link) a link gave the length of the
			// link itself, which is not the length of anything in the log. A link to nothing
			// fails here with `NotFound`, where it failed at the open that followed.
			if let Some(file_id) = self.opts.extract_vlog_file_id(&entry.name.to_string_lossy()) {
				let path = self.path.join(&entry.name);
				let len = self.fs.file_len(&path)?;
				found.push((file_id, path, len));
			}
		}
		// A listing has no order.
		found.sort_by_key(|(file_id, ..)| *file_id);
		let Some(newest_id) = found.iter().map(|(file_id, ..)| *file_id).max() else {
			return Ok(None);
		};

		let mut newest = None;
		// The files that a crash left without a complete header. Nothing is renamed until every
		// other file has opened: the files come in whatever order the directory lists them, and a
		// log that then fails on a key that does not fit must leave the directory as it was.
		let mut torn: Vec<(u32, PathBuf)> = Vec::new();
		// The order `cleanup_obsolete_files` takes them in
		let mut files_map = self.files_map.write();
		let mut file_handles = self.file_handles.write();

		for (file_id, file_path, file_size) in found {
			let is_newest = file_id == newest_id;
			// A file of an encrypted log has its header synced when it is made: only the newest
			// can be cut off in it.
			let can_be_torn = is_newest || self.encryption().is_none();
			if can_be_torn && self.is_torn(&file_path, file_id, file_size)? {
				torn.push((file_id, file_path));
				continue;
			}

			// Pre-open the file handle and validate header. The newest file of a log that is not
			// encrypted is the one that is appended to next, so it is opened to do that too, and
			// its writer takes the handle over: it is not opened a second time. It was listed, so
			// it is there, and one that is gone since is a `NotFound` as it was when this opened it
			// to read (`File::open`), and not a new empty file.
			let opened = if is_newest && self.encryption().is_none() {
				self.fs.open_append_existing(&file_path)
			} else {
				self.fs.open_read(&file_path)
			};
			match opened {
				Ok(file) => {
					let mut cipher = None;
					// Validate file header for existing files
					if file_size > 0 {
						let mut header_data = vec![0u8; VLogFileHeader::SIZE];
						vfs::File::read_at(&*file, 0, &mut header_data)?;
						// A header that is all zeros did not reach the disk, whatever follows
						// it: the entries after it were never synced either, since an fsync of
						// the file writes its header with them, and no record that is durable
						// points to one. The file is not open when it is set aside.
						if self.encryption().is_none() && header_data.iter().all(|&byte| byte == 0)
						{
							torn.push((file_id, file_path));
							continue;
						}
						let header = VLogFileHeader::decode(&header_data)?;
						cipher = self.open_layout(&*file, &header, file_id, true)?;
						if cipher.is_none() {
							header.validate(file_id)?;
						}
					} else if self.encryption().is_some() {
						return Err(Error::Corruption(format!(
							"VLog file {file_id} of a sealed log is empty and is not its newest file"
						)));
					}

					if is_newest {
						newest = Some(Newest {
							id: file_id,
							sealed: cipher.is_some(),
							file: Some(Arc::clone(&file)),
						});
					}
					file_handles.insert(
						file_id,
						Arc::new(OpenFile {
							file,
							cipher,
						}),
					);

					// Also register in files_map
					files_map
						.insert(file_id, Arc::new(VLogFile::new(file_id, file_path, file_size)));
				}
				Err(e) => {
					tracing::error!("Failed to pre-open VLog file {file_path:?}: {e}");
					return Err(e.into());
				}
			}
		}

		// Every other file opened, so no key was refused and nothing is in doubt but these files.
		for (file_id, file_path) in &torn {
			self.set_aside(*file_id, file_path)?;
			if *file_id == newest_id {
				newest = Some(Newest {
					id: newest_id,
					sealed: true,
					file: None,
				});
			}
		}

		Ok(newest)
	}

	/// Checks what the header of a file says about how its entries are stored against what the
	/// log is configured for, and opens a sealed file. Returns the cipher that opens its entries,
	/// or `None` for the plaintext file of a log without encryption, whose header the caller goes
	/// on to check.
	///
	/// A log reads files of one kind. One that is configured with encryption refuses a plaintext
	/// file and one that is not refuses a sealed file, and the file is the same afterwards:
	/// neither is damage, and neither is a reason to repair. `prepare` asks the key manager to
	/// make the key of the file available first, which is for when the log is opened and not for
	/// a read, which a manager must not make wait.
	fn open_layout(
		&self,
		file: &dyn vfs::File,
		header: &VLogFileHeader,
		file_id: u32,
		prepare: bool,
	) -> Result<Option<FileCipher>> {
		let sealed = header.version == sealed::SEALED_VERSION;
		let Some(context) = self.encryption() else {
			return if sealed {
				Err(EncryptionError::NotConfigured.into())
			} else {
				Ok(None)
			};
		};
		if !sealed {
			return Err(if header.version == VLOG_FORMAT_VERSION {
				EncryptionError::PlaintextInEncryptedDb.into()
			} else {
				Error::Corruption(format!(
					"Incompatible VLog file version (file_id={file_id}): {}",
					header.version
				))
			});
		}
		let mut head = vec![0u8; sealed::SEALED_HEADER_LEN];
		let read = vfs::File::read_up_to(file, 0, &mut head)?;
		head.truncate(read);
		sealed::open_header(context, file_id, &head, prepare).map(Some)
	}

	/// Whether `path` is what a crash leaves of a file whose header was not written back, see
	/// [`sealed::is_torn`] and [`VLogFileHeader::is_torn`]. Only the newest file of an encrypted
	/// log is asked.
	fn is_torn(&self, path: &Path, file_id: u32, file_size: u64) -> Result<bool> {
		if self.encryption().is_some() {
			if file_size > sealed::SEALED_HEADER_LEN as u64 {
				return Ok(false);
			}
			return Ok(sealed::is_torn(&self.fs.read(path)?));
		}
		if file_size == 0 || file_size > VLogFileHeader::SIZE as u64 {
			return Ok(false);
		}
		Ok(VLogFileHeader::is_torn(&self.fs.read(path)?, file_id))
	}

	/// Renames a file that a crash left without a complete header, none of whose entries is
	/// durable, to a name that the log does not read.
	fn set_aside(&self, file_id: u32, path: &Path) -> Result<()> {
		let to = sealed::set_aside_in(&*self.fs, path)?;
		tracing::warn!(
			"VLog file {file_id} was left by a crash without a complete header and holds no entry \
			 that is durable: it was set aside as {to:?}"
		);
		if let Err(e) = self.fs.sync_dir(&self.path) {
			tracing::warn!("Failed to sync the VLog directory after setting a file aside: {e}");
		}
		Ok(())
	}

	/// Keeps every append, and with it every roll over to a new file, out until the guard is
	/// dropped or given to [`reload`](Self::reload). A restore holds it from before it removes the
	/// directory of the log: a group that began before the restore and writes its values meanwhile
	/// would create a file in the directory while it is removed, which fails the restore half
	/// way, or in the restored one, under the id of a restored file or one that it does not have.
	///
	/// The lock sits below the WAL's, as a rotation takes it: the holder takes no other lock until
	/// it has given the guard to `reload`.
	pub(crate) fn hold_writer(&self) -> RwLockWriteGuard<'_, Option<VLogWriter>> {
		self.writer.write()
	}

	/// Replaces everything the log knows about its files by what its directory holds now, after
	/// a restore replaced the directory. The writer, the handles and the file registry belong to
	/// files that are gone, and a file id that the restored directory has again would otherwise
	/// be read through, and appended to by, the old file.
	///
	/// The log has no writer afterwards, so the next append starts a file of its own, one past
	/// the highest id found, and never one of the restored files. `writer` is the guard that
	/// [`hold_writer`](Self::hold_writer) returned before the directory was replaced, and keeps
	/// appends out until the log is loaded.
	pub(crate) fn reload(
		&self,
		mut writer: RwLockWriteGuard<'_, Option<VLogWriter>>,
	) -> Result<()> {
		// Before the directory is read: a checkpoint without a value log leaves none.
		self.fs.create_dir_all(&self.path)?;

		// What the old writer still buffers goes to a file that was removed.
		*writer = None;
		self.unsynced.lock().clear();
		// A failed fsync lost pages of files that are gone: the files that replace them have had
		// no fsync, and the first sync covers them, as below.
		{
			let _serial = self.sync_serial.lock();
			self.sync_failed.store(false, Ordering::Release);
		}
		{
			let mut files_map = self.files_map.write();
			let mut file_handles = self.file_handles.write();
			files_map.clear();
			file_handles.clear();
			self.generation.store(crate::cache::next_instance_id(), Ordering::SeqCst);
		}

		let highest = self.load_files()?;
		self.next_file_id.store(highest.map_or(1, |newest| newest.id + 1), Ordering::SeqCst);
		self.active_writer_id.store(0, Ordering::SeqCst);
		// The files were copied in, and neither they nor the directory entries are known to be
		// on disk: the first sync covers them.
		self.appended.mark();
		self.created.mark();
		Ok(())
	}

	/// The key under which the block cache keeps the values of file `file_id` of the log as it is
	/// now: a reload makes the same file id another file, and another log has files of the same
	/// ids, so the generation, which is not the generation of any other log, is part of the key.
	/// The generation takes the upper 32 bits, which a process would need to make over four
	/// billion tables and logs to fill.
	pub(crate) fn cache_id(&self, file_id: u32) -> u64 {
		(self.generation.load(Ordering::SeqCst) << 32) | u64::from(file_id)
	}

	/// Retrieves (and caches) an open file handle for the given file ID
	fn get_file_handle(&self, file_id: u32) -> Result<Arc<OpenFile>> {
		let generation = {
			let handles = self.file_handles.read();
			if let Some(handle) = handles.get(&file_id) {
				return Ok(Arc::clone(handle));
			}
			self.generation.load(Ordering::SeqCst)
		};

		// Open and validate the file
		let file_path = self.vlog_file_path(file_id);
		let file = self.fs.open_read(&file_path).map_err(|e| {
			tracing::error!(
				"Failed to open VLog file: file_id={}, path={:?}, error={}",
				file_id,
				file_path,
				e
			);
			Error::Other(format!(
				"Failed to open VLog file (file_id={}, path={:?}): {}",
				file_id, file_path, e
			))
		})?;

		// Validate file header for existing files
		let file_size = file.len().map_err(|e| {
			tracing::error!(
				"Failed to get VLog file metadata: file_id={}, path={:?}, error={}",
				file_id,
				file_path,
				e
			);
			Error::Other(format!(
				"Failed to get VLog file metadata (file_id={}, path={:?}): {}",
				file_id, file_path, e
			))
		})?;

		let mut cipher = None;
		if file_size > 0 {
			// Only validate header if file has content (not a new empty file)
			let mut header_data = vec![0u8; VLogFileHeader::SIZE];
			vfs::File::read_at(&*file, 0, &mut header_data).map_err(|e| {
				tracing::error!(
					"Failed to read VLog header: file_id={}, path={:?}, error={}",
					file_id,
					file_path,
					e
				);
				Error::Other(format!(
					"Failed to read VLog header (file_id={}, path={:?}): {}",
					file_id, file_path, e
				))
			})?;

			let header = VLogFileHeader::decode(&header_data).map_err(|e| {
				tracing::error!(
					"Failed to decode VLog header: file_id={}, path={:?}, error={}",
					file_id,
					file_path,
					e
				);
				Error::Other(format!(
					"Failed to decode VLog header (file_id={}, path={:?}): {}",
					file_id, file_path, e
				))
			})?;

			// A sealed file is checked as a whole when it is opened, and what it says about how
			// its entries are stored is checked against what the log is configured for.
			cipher = self.open_layout(&*file, &header, file_id, false)?;
			if cipher.is_none() {
				if !header.is_compatible() {
					tracing::error!(
						"Incompatible VLog file version: file_id={}, path={:?}, version={} (expected {})",
						file_id, file_path, header.version, VLOG_FORMAT_VERSION
					);
					return Err(Error::Corruption(format!(
						"Incompatible VLog file version (file_id={}, path={:?}): {} (expected {})",
						file_id, file_path, header.version, VLOG_FORMAT_VERSION
					)));
				}

				// Validate header against current options
				header.validate(file_id).map_err(|e| {
					tracing::error!(
						"VLog header validation failed: file_id={}, path={:?}, error={}",
						file_id,
						file_path,
						e
					);
					Error::Other(format!(
						"VLog header validation failed (file_id={}, path={:?}): {}",
						file_id, file_path, e
					))
				})?;
			}
		} else if self.encryption().is_some() {
			return Err(Error::Corruption(format!("VLog file {file_id} of a sealed log is empty")));
		}

		let handle = Arc::new(OpenFile {
			file,
			cipher,
		});

		let mut handles = self.file_handles.write();
		// The file may have been replaced while it was opened: the caller reads what it opened,
		// and it is not kept for the file that has this id now.
		if self.generation.load(Ordering::SeqCst) != generation {
			return Ok(handle);
		}
		// Another thread might have inserted it while we were opening the file
		let entry = handles.entry(file_id).or_insert_with(|| Arc::clone(&handle));
		Ok(Arc::clone(entry))
	}

	/// Retrieves a value using a ValuePointer
	pub(crate) fn get(&self, pointer: &ValuePointer) -> Result<Value> {
		// Fixed before anything is read: a restore that reloads the log while this call reads a
		// file of the old one leaves what it reads under the key of the old log.
		let cache_id = self.cache_id(pointer.file_id);

		// Check unified block cache first
		if let Some(cached_value) = self.opts.block_cache.get_vlog(cache_id, pointer.offset) {
			return Ok(cached_value);
		}

		let handle = self.get_file_handle(pointer.file_id)?;
		match (&handle.cipher, pointer.is_sealed()) {
			(Some(cipher), true) => {
				return self.get_sealed(&*handle.file, cipher, pointer, cache_id)
			}
			(None, false) => {}
			(Some(_), false) | (None, true) => {
				// A pointer and a file that disagree about whether entries are sealed: the
				// entry is not read as the one kind or the other.
				return Err(Error::Corruption(format!(
					"The pointer and the file disagree about whether the entry is sealed \
					 (file_id={}, offset={})",
					pointer.file_id, pointer.offset
				)));
			}
		}
		let file = &*handle.file;

		// Read the entire entry in a single call using sizes from the pointer
		let key_len = pointer.key_size;
		let value_len = pointer.value_size;
		let total_size = pointer.total_entry_size();

		// What a pointer says is not trusted to size an allocation: a length that does not fit in
		// memory, or that is longer than the file it points into, is an entry that the end of its
		// file cuts short, and is reported as one.
		let cut_short = |read: u64| {
			Error::Corruption(format!(
				"VLog entry cut short by the end of its file (file_id={}, offset={}): read {} of {} \
				 bytes",
				pointer.file_id, pointer.offset, read, total_size
			))
		};
		let size = usize::try_from(total_size).map_err(|_| cut_short(0))?;
		// Only where the address space is 32 bits: there a length that is below `usize::MAX` can
		// still be more than the process can allocate, and an allocation that fails aborts it. On a
		// 64-bit target the read is what it always was, with no call to ask for the length.
		if let Some(file_len) =
			entry_bound(total_size, pointer.offset, BOUND_ENTRIES, || vfs::File::size(file))?
		{
			return Err(cut_short(file_len.saturating_sub(pointer.offset)));
		}

		// `read_at` may return fewer bytes than asked for, and returns none at the end of the
		// file: read until the entry is complete or the file ends. An entry that the end of its
		// file cuts short is an error, not a value whose tail is the zeros of the buffer: without
		// checksum verification nothing else would tell them apart from what was written.
		let mut entry_data_vec = vec![0u8; size];
		let mut filled = 0;
		while filled < entry_data_vec.len() {
			let read = vfs::File::read_at(
				file,
				pointer.offset + filled as u64,
				&mut entry_data_vec[filled..],
			)
			.map_err(|e| {
				tracing::error!("Failed to read VLog entry: pointer={:?}, error={}", pointer, e);
				Error::Other(format!(
					"Failed to read VLog entry (file_id={}, offset={}, key_size={}, value_size={}): {}",
					pointer.file_id, pointer.offset, pointer.key_size, pointer.value_size, e
				))
			})?;
			if read == 0 {
				break;
			}
			filled += read;
		}
		if filled < entry_data_vec.len() {
			tracing::error!(
				"VLog entry cut short by the end of its file: pointer={:?}, read {} of {} bytes",
				pointer,
				filled,
				total_size
			);
			return Err(Error::Corruption(format!(
				"VLog entry cut short by the end of its file (file_id={}, offset={}): read {} of {} \
				 bytes",
				pointer.file_id, pointer.offset, filled, total_size
			)));
		}

		// Verify header sizes
		let header_key_len = u32::from_be_bytes([
			entry_data_vec[0],
			entry_data_vec[1],
			entry_data_vec[2],
			entry_data_vec[3],
		]);
		let header_value_len = u32::from_be_bytes([
			entry_data_vec[4],
			entry_data_vec[5],
			entry_data_vec[6],
			entry_data_vec[7],
		]);

		if header_key_len != key_len || header_value_len != value_len {
			tracing::error!(
				"VLog header size mismatch: pointer={:?}, header_key_len={}, header_value_len={}",
				pointer,
				header_key_len,
				header_value_len
			);
			return Err(Error::Corruption(format!(
				"Header size mismatch (file_id={}, offset={}): expected key {} value {}, got key {} value {}",
				pointer.file_id, pointer.offset, key_len, value_len, header_key_len, header_value_len
			)));
		}

		// Parse offsets from the buffer
		let key_start = 8;
		let value_start = key_start + key_len as usize;
		let crc_start = value_start + value_len as usize;

		let stored_crc32 = u32::from_be_bytes([
			entry_data_vec[crc_start],
			entry_data_vec[crc_start + 1],
			entry_data_vec[crc_start + 2],
			entry_data_vec[crc_start + 3],
		]);

		// Checksum verification
		if self.checksum_level != VLogChecksumLevel::Disabled && stored_crc32 != pointer.checksum {
			tracing::error!(
				"VLog CRC32 mismatch (stored vs pointer): pointer={:?}, stored_crc32={}",
				pointer,
				stored_crc32
			);
			return Err(Error::Corruption(format!(
				"CRC32 mismatch (file_id={}, offset={}): expected {}, got {} (stored in file)",
				pointer.file_id, pointer.offset, pointer.checksum, stored_crc32
			)));
		}

		if self.checksum_level == VLogChecksumLevel::Full {
			let key = &entry_data_vec[key_start..value_start];
			let value = &entry_data_vec[value_start..crc_start];
			let mut hasher = Hasher::new();
			hasher.update(key);
			hasher.update(value);
			let calculated_crc32 = hasher.finalize();
			if calculated_crc32 != pointer.checksum {
				tracing::error!(
					"VLog Key+Value CRC32 mismatch: pointer={:?}, calculated_crc32={}",
					pointer,
					calculated_crc32
				);
				return Err(Error::Corruption(format!(
					"Key+Value CRC32 mismatch (file_id={}, offset={}): expected {}, calculated {}",
					pointer.file_id, pointer.offset, pointer.checksum, calculated_crc32
				)));
			}
		}

		// Extract value slice from entry data
		let value_bytes = entry_data_vec[value_start..crc_start].to_vec();

		// Cache the value in unified block cache for future reads
		self.opts.block_cache.insert_vlog(cache_id, pointer.offset, value_bytes.clone());

		Ok(value_bytes)
	}

	/// Retrieves a value using a ValuePointer into `out`, reusing `out`'s capacity on cache hits.
	pub(crate) fn get_into(&self, pointer: &ValuePointer, out: &mut Vec<u8>) -> Result<()> {
		let cache_id = self.cache_id(pointer.file_id);
		if self.opts.block_cache.get_vlog_into(cache_id, pointer.offset, out) {
			return Ok(());
		}
		let value = self.get(pointer)?;
		out.clear();
		out.extend_from_slice(&value);
		Ok(())
	}

	/// Reads the value of a sealed entry. What is read is checked against the pointer's CRC-32 and
	/// authenticated before anything of it is looked at, whatever `checksum_level` is: the tag
	/// is the verification, and it is not an option.
	fn get_sealed(
		&self,
		file: &dyn vfs::File,
		cipher: &FileCipher,
		pointer: &ValuePointer,
		cache_id: u64,
	) -> Result<Value> {
		let cut_short = || {
			Error::Corruption(format!(
				"VLog entry cut short by the end of its file (file_id={}, offset={})",
				pointer.file_id, pointer.offset
			))
		};

		// The size of the allocation is bounded by the file, not by what a pointer claims.
		let total = pointer.total_entry_size();
		let end = pointer.offset.checked_add(total).ok_or_else(cut_short)?;
		if end > file.size()? {
			return Err(cut_short());
		}
		let size = usize::try_from(total).map_err(|_| cut_short())?;
		let mut stored = Vec::new();
		stored.try_reserve_exact(size).map_err(|e| Error::Io(Arc::new(io::Error::from(e))))?;
		stored.resize(size, 0);
		match vfs::File::read_exact_at(file, pointer.offset, &mut stored) {
			Ok(()) => {}
			Err(Error::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
				return Err(cut_short())
			}
			Err(e) => return Err(e),
		}

		let value = sealed::open_entry(cipher, pointer, stored)?;
		self.opts.block_cache.insert_vlog(cache_id, pointer.offset, value.clone());
		Ok(value)
	}

	/// The handle of file `file_id`, or `None` if the log has no such file. A file that was
	/// listed when the log was opened has a handle; any other is looked for in the file system.
	fn handle_if_present(&self, file_id: u32) -> Result<Option<Arc<OpenFile>>> {
		if let Some(handle) = self.file_handles.read().get(&file_id) {
			return Ok(Some(Arc::clone(handle)));
		}
		if !self.fs.try_exists(&self.vlog_file_path(file_id))? {
			return Ok(None);
		}
		self.get_file_handle(file_id).map(Some)
	}

	/// Whether the entry that `pointer` names is in the log, and whole: its file is there, the
	/// entry lies inside the file, and what is stored is what the pointer says. `Ok(false)` is
	/// any of those that does not hold, and says nothing of why. An error is a read that failed,
	/// or a pointer and a file that disagree about whether entries are sealed.
	///
	/// A plaintext entry is held to the lengths in its header, to the checksum stored with it,
	/// and to the checksum of its key and value, which is calculated whatever
	/// [`VLogChecksumLevel`] the log reads with: it is the only test of a page that is missing in
	/// the middle of a value. A sealed entry is held to its length and to the checksum of what is
	/// stored, which is the one thing that can be checked without the key.
	///
	/// The entry is read into `buf`, once, and `buf` is kept for the next one. Nothing is put in
	/// the block cache, and nothing is allocated for the entry if `buf` has room for it.
	pub(crate) fn check_entry(&self, pointer: &ValuePointer, buf: &mut Vec<u8>) -> Result<bool> {
		let Some(handle) = self.handle_if_present(pointer.file_id)? else {
			return Ok(false);
		};
		let sealed = match (&handle.cipher, pointer.is_sealed()) {
			(Some(_), true) => true,
			(None, false) => false,
			(Some(_), false) | (None, true) => {
				return Err(Error::Corruption(format!(
					"The pointer and the file disagree about whether the entry is sealed \
					 (file_id={}, offset={})",
					pointer.file_id, pointer.offset
				)));
			}
		};
		let file = &*handle.file;

		// What a pointer says is not trusted to size an allocation: an entry that does not fit in
		// memory, or that ends past its file, is not in the log. The length of the file is asked
		// for an entry of 16 MiB or more only, on every target, so that no pointer can ask for
		// more than that of memory that the file does not have.
		let total = pointer.total_entry_size();
		if pointer.offset.checked_add(total).is_none() {
			return Ok(false);
		}
		let Ok(size) = usize::try_from(total) else {
			return Ok(false);
		};
		if entry_bound(total, pointer.offset, true, || vfs::File::size(file))?.is_some() {
			return Ok(false);
		}
		if buf.len() < size {
			buf.try_reserve(size - buf.len())
				.map_err(|e| Error::Io(Arc::new(io::Error::from(e))))?;
			buf.resize(size, 0);
		}

		// `read_at` may return fewer bytes than asked for, and none at the end of the file.
		let entry = &mut buf[..size];
		let mut filled = 0;
		while filled < size {
			let read =
				vfs::File::read_at(file, pointer.offset + filled as u64, &mut entry[filled..])?;
			if read == 0 {
				return Ok(false);
			}
			filled += read;
		}

		if sealed {
			return Ok(crc32fast::hash(entry) == pointer.checksum);
		}
		let lens = |at: usize| {
			u32::from_be_bytes([entry[at], entry[at + 1], entry[at + 2], entry[at + 3]])
		};
		if lens(0) != pointer.key_size || lens(4) != pointer.value_size {
			return Ok(false);
		}
		let crc_start = size - 4;
		if lens(crc_start) != pointer.checksum {
			return Ok(false);
		}
		Ok(crc32fast::hash(&entry[8..crc_start]) == pointer.checksum)
	}

	/// Makes the files `file_ids` durable, and then the directory they are in: what the open does
	/// for the files that the records it keeps point into, before it starts another segment of
	/// the log, so that a record of an older segment never names a value that a crash can take.
	/// A file that is not there is an error.
	///
	/// The file that is appended to and the files that were rolled over from are synced as every
	/// sync does it ([`sync`](Self::sync)), which keeps the count of what an fsync covers: the
	/// fsync that the open makes next, through [`sync_dirty`](Self::sync_dirty), finds nothing
	/// left to do.
	pub(crate) fn sync_files(&self, file_ids: &BTreeSet<u32>) -> Result<()> {
		if file_ids.is_empty() {
			return Ok(());
		}
		let active = self.active_writer_id.load(Ordering::SeqCst);
		for &file_id in file_ids.iter().filter(|&&file_id| file_id != active) {
			let handle = self.get_file_handle(file_id)?;
			self.fsync(file_id, &*handle.file)?;
		}
		self.sync()?;
		self.sync_dir()?;
		Ok(())
	}

	/// Keeps every file in place until the pin is dropped. A checkpoint takes one while it holds
	/// the manifest and copies the files after releasing it, so a flush or a compaction cannot
	/// delete files that the checkpointed tables point into in between.
	pub(crate) fn pin_files(&self) -> VLogPin<'_> {
		self.pins.fetch_add(1, Ordering::SeqCst);
		VLogPin(self)
	}

	/// Pins the files that the values of a commit group are about to be written to. The flusher
	/// takes it before it appends the first value of the group and drops it once the group is in
	/// a memtable or a table, or, for a group that failed with its records logged, once the
	/// active memtable accounts for the files they point into. Every value of the group goes to
	/// the file that is active now or a later one.
	///
	/// Between the first append and that point the values are in no table and in no memtable, so
	/// the bound of a cleanup (`lsm::vlog_floor`) cannot see them. [`unregistered_floor`]
	/// (Self::unregistered_floor) makes the cleanup keep them.
	pub(crate) fn pin_group(&self) -> VLogGroupPin<'_> {
		self.group_floor.store(self.active_writer_id.load(Ordering::SeqCst), Ordering::SeqCst);
		VLogGroupPin(self)
	}

	/// The smallest file that a cleanup may not delete, whatever the tables and memtables say:
	/// the files of values that no table and no memtable points into yet. A cleanup reads it
	/// before it reads them, for the argument that it is enough:
	///
	/// - A group that started before the active file was read has its pin read next, if it is still
	///   held. Its values are in files at or above the pin.
	/// - A group that released its pin before it was read has put its values into a memtable or a
	///   table first (the release is sequentially consistent, and the reads of the bound that
	///   follow see what came before it).
	/// - A group that starts after the pin was read appends to files at or above the active file
	///   that was read first, since the active file only grows.
	///
	/// The active file comes first for the third case: read after the pin, it could already be a
	/// file that such a group rolled over to, and the files of the group below it would go.
	pub(crate) fn unregistered_floor(&self) -> u32 {
		let active = self.active_writer_id.load(Ordering::SeqCst);
		active.min(self.group_floor.load(Ordering::SeqCst))
	}

	/// Cleans up obsolete vlog files based on the global minimum file id that anything points into.
	///
	/// A vlog file is safe to delete when:
	/// - Its file_id < min_oldest_vlog (no SST and no unflushed memtable has a value in it, see
	///   `lsm::vlog_floor`, and no commit group is about to, see
	///   [`unregistered_floor`](Self::unregistered_floor))
	/// - It is below the active writer's file
	/// - No iterators are active
	/// - No [`VLogPin`] is held
	///
	/// This implements the "global minimum" GC approach where files are deleted
	/// once nothing can possibly reference them. If iterators are active or a pin is
	/// held, cleanup is skipped and will be retried on the next GC trigger.
	///
	/// It picks the files to delete by id and removes them by id, and it takes the bound as an
	/// argument, so it must not overlap a [`reload`](Self::reload): ids mean other files after it,
	/// and a bound from before it is no bound for the restored tables. Every caller holds
	/// `flush_lock` or `compactions` (or runs before the tree is shared), which a restore holds
	/// from before it replaces the directory until it is done.
	pub(crate) fn cleanup_obsolete_files(&self, min_oldest_vlog: u32) -> Result<()> {
		if self.pins.load(Ordering::SeqCst) > 0 {
			return Ok(());
		}

		// The bound is capped at the active file, the newest one that can be deleted: a rollover
		// registers the file it creates before it makes it the active one, and a bound of
		// `u32::MAX` (nothing points into the log) would otherwise take that file along.
		let bound = min_oldest_vlog.min(self.active_writer_id.load(Ordering::SeqCst));

		// Collect files that are safe to delete
		let to_delete: Vec<u32> = {
			let files_map = self.files_map.read();
			files_map.keys().filter(|&&id| id < bound).copied().collect()
		};

		if to_delete.is_empty() {
			return Ok(());
		}

		// No active iterators, safe to delete
		self.unsynced.lock().retain(|(id, _)| !to_delete.contains(id));
		let mut files_map = self.files_map.write();
		let mut file_handles = self.file_handles.write();

		for file_id in to_delete {
			// The handle goes first: a file system that keeps one handle per path (a browser's)
			// does not remove a file that is open, and the readers that hold a clone of it are
			// the only ones left.
			file_handles.remove(&file_id);
			let Some(vlog_file) = files_map.get(&file_id) else {
				continue;
			};
			match self.fs.remove_file(&vlog_file.path) {
				Ok(()) => {
					tracing::debug!(
						"Deleted obsolete VLog file: file_id={}, path={:?}",
						file_id,
						vlog_file.path
					);
					files_map.remove(&file_id);
				}
				// Already gone, as a crash between a removal and its next fsync leaves it, or
				// a removal that was made by hand.
				Err(e) if e.kind() == io::ErrorKind::NotFound => {
					files_map.remove(&file_id);
				}
				Err(e) => {
					// The file is still there, so it stays in the registry: it is found again,
					// and removed, by the next cleanup. Forgetting it here would leak it until
					// the log is opened again. Continue with the other files, don't fail the
					// entire cleanup.
					tracing::error!(
						"Failed to delete obsolete VLog file: file_id={}, path={:?}, error={}",
						file_id,
						vlog_file.path,
						e
					);
				}
			}
		}

		Ok(())
	}

	/// Flushes buffered data in the active writer to OS page cache.
	pub(crate) fn flush(&self) -> Result<()> {
		let mut guard = self.writer.write();
		if let Some(ref mut writer) = *guard {
			writer.flush()?;
		}
		Ok(())
	}

	/// Syncs all data to disk: the active file and every file the writer rolled over from
	/// that is not synced yet, then the directory if a file was created since it was last synced.
	///
	/// The first fsync of a file that fails ends the log: it and every later sync fail, until the
	/// log is opened again or [reloaded](Self::reload). An fsync that fails has lost the pages it
	/// could not write, and the kernel no longer counts them as dirty, so a later fsync of the
	/// same file reports success without them; a value written before the failure can then never
	/// be made durable, and nothing that points at one may be told it is. The failure stops the
	/// database (see [`set_error_handler`](Self::set_error_handler)), and recovery decides what
	/// survived.
	///
	/// The write lock is held only for the BufWriter flush (draining the internal buffer to
	/// OS page cache, ~microseconds). The expensive fsync is performed outside the lock using
	/// pre-cloned file descriptors, allowing concurrent VLog appends to proceed.
	pub(crate) fn sync(&self) -> Result<()> {
		// Phase 1: Under write lock — flush BufWriter and grab the sync fds, and what they cover
		let (queued, active, appended, created) = {
			let mut guard = self.writer.write();
			let active = match guard.as_mut() {
				Some(writer) => {
					writer.flush()?;
					Some((writer.file_id, writer.sync_fd()))
				}
				None => None,
			};
			(self.unsynced.lock().clone(), active, self.appended.count(), self.created.count())
			// write lock released here
		};
		#[cfg(test)]
		{
			let gap = self.sync_gap.lock().clone();
			if let Some(gap) = gap {
				gap();
			}
		}

		// Phase 2: Outside lock — fsync to disk (slow, doesn't block appends). A rolled-over
		// file leaves the queue only once its fsync has succeeded, and a sync that overlaps
		// another fsyncs the file itself, as the other's fsync may still be in flight. Only a file
		// that was queued when the fds were taken may leave it: the active file is queued
		// again by a rollover after that, for bytes this fsync may not cover.
		//
		// An entry is the descriptor that was queued, not its id: a sync that outlasts a reload
		// fsyncs a file that is gone, and the log may have queued a file of the same id since,
		// whose bytes this fsync did not cover. `queued` keeps the descriptors alive, so one
		// cannot be freed and its address reused while this runs.
		for (file_id, fd) in &queued {
			self.fsync(*file_id, &**fd)?;
			self.unsynced.lock().retain(|(_, entry)| !Arc::ptr_eq(entry, fd));
		}
		if let Some((file_id, fd)) = &active {
			self.fsync(*file_id, &**fd)?;
		}

		// A file's fsync does not make its name durable everywhere: the directory is fsynced
		// once for the files created since it last was, and not by the commit that rolled the
		// log over, which fsyncs nothing.
		if !self.created.covers(created) {
			self.sync_dir()?;
			self.created.settle(created);
		}
		self.appended.settle(appended);
		Ok(())
	}

	/// Hands the log the handler of the tree that owns it: the first failed fsync records a fatal
	/// error there, so that every commit fails until the database is opened again.
	pub(crate) fn set_error_handler(&self, handler: Arc<BackgroundErrorHandler>) {
		let _ = self.error_handler.set(handler);
	}

	/// Whether an fsync of the log has failed, after which every sync fails.
	pub(crate) fn sync_failed(&self) -> bool {
		self.sync_failed.load(Ordering::Acquire)
	}

	/// Runs one fsync of a file of the log: refused once one has failed, and the end of the log if
	/// it fails.
	///
	/// The fsyncs of the log never overlap. The kernel reports a writeback error to a single
	/// fsync of a file description, so an fsync that overlaps the failing one, or that starts after
	/// it failed and before the failure is recorded, can report success for pages that never
	/// reached the disk. `sync_serial` is held from the check of `sync_failed` to the setting of
	/// it, so every fsync either completes before the failing one starts or is refused.
	///
	/// A leaf lock: nothing is taken under it but the handler's, so it is safe to take under the
	/// WAL lock, as a rotation does.
	fn guarded_sync(&self, fsync: impl FnOnce() -> Result<()>) -> Result<()> {
		#[cfg(test)]
		self.sync_arrivals.fetch_add(1, Ordering::SeqCst);
		let _serial = self.sync_serial.lock();
		if self.sync_failed() {
			return Err(sync_poisoned());
		}
		#[cfg(test)]
		{
			let observer = self.sync_observer.lock().clone();
			if let Some(observer) = observer {
				observer();
			}
		}
		let result = fsync();
		if let Err(error) = &result {
			self.sync_failed.store(true, Ordering::Release);
			if let Some(handler) = self.error_handler.get() {
				handler.set_error(
					Error::DatabaseStopped(format!(
						"an fsync of the value log failed, so the values it was to make durable \
						 may be lost, and recovery decides what survived when the database is \
						 opened again: {error}"
					)),
					BackgroundErrorReason::ValueLogSync,
				);
			}
		}
		result
	}

	/// fsyncs the log's directory. A failure leaves the log as it was, and the next sync fsyncs
	/// the directory again: it loses no value, as only a file's own fsync drops its pages.
	fn sync_dir(&self) -> Result<()> {
		Ok(self.fs.sync_dir(&self.path)?)
	}

	/// [`sync`](Self::sync), unless every entry appended is covered by an fsync that has
	/// completed: then there is nothing to make durable, and no fsync is made.
	///
	/// A sync still in flight does not count, so a caller that arrives while one runs fsyncs
	/// itself. What counts is what this process appended, and what the active file held when the
	/// log was opened.
	///
	/// A log whose fsync failed is refused even when nothing is unsynced, as every sync of it is:
	/// a rotation syncs it through here before it seals a segment whose records point into it.
	pub(crate) fn sync_dirty(&self) -> Result<()> {
		if self.sync_failed() {
			return Err(sync_poisoned());
		}
		if self.appended.is_clean() {
			return Ok(());
		}
		self.sync()
	}

	fn fsync(&self, file_id: u32, fd: &dyn FsFile) -> Result<()> {
		#[cfg(test)]
		let len = fd.len().unwrap_or(0);
		self.guarded_sync(|| {
			#[cfg(test)]
			if self.fail_next_sync.swap(false, Ordering::SeqCst) {
				return Err(Error::Io(Arc::new(io::Error::other("injected fsync failure"))));
			}
			fd.sync()?;
			Ok(())
		})?;
		tracing::trace!("Synced VLog file {file_id}");
		#[cfg(test)]
		{
			SYNCED_VLOG_FILES.lock().push(self.vlog_file_path(file_id));
			SYNCED_VLOG_LENGTHS.lock().push((self.vlog_file_path(file_id), len));
		}
		Ok(())
	}

	/// Test-only: how many times the log's directory has been fsynced, by anyone.
	#[cfg(test)]
	pub(crate) fn dir_syncs(&self) -> usize {
		let dir = self.path.canonicalize().unwrap_or_else(|_| self.path.clone());
		crate::lsm::SYNCED_DIRECTORIES.lock().iter().filter(|synced| **synced == dir).count()
	}

	/// Test-only: the ids of the files that wait for an fsync because the writer rolled over from
	/// them, in the order they were queued.
	#[cfg(test)]
	pub(crate) fn unsynced_file_ids(&self) -> Vec<u32> {
		self.unsynced.lock().iter().map(|(id, _)| *id).collect()
	}

	/// Test-only: makes the next fsync of [`sync`](Self::sync) fail, as a failing disk does.
	#[cfg(test)]
	pub(crate) fn fail_next_sync(&self) {
		self.fail_next_sync.store(true, Ordering::SeqCst);
	}

	/// Test-only: how many calls reached the fsync of a file, the ones waiting for another
	/// included.
	#[cfg(test)]
	pub(crate) fn sync_arrivals(&self) -> usize {
		self.sync_arrivals.load(Ordering::SeqCst)
	}

	/// Test-only: sets what runs inside every fsync of a file, with the lock that keeps fsyncs
	/// from overlapping held, before the fsync (and before an injected failure).
	#[cfg(test)]
	pub(crate) fn set_sync_observer(&self, observer: Option<Arc<dyn Fn() + Send + Sync>>) {
		*self.sync_observer.lock() = observer;
	}

	/// Test-only: sets what runs in [`sync`](Self::sync) between taking the fds and fsyncing
	/// them, with no lock held.
	#[cfg(test)]
	pub(crate) fn set_sync_gap(&self, gap: Option<Arc<dyn Fn() + Send + Sync>>) {
		*self.sync_gap.lock() = gap;
	}

	/// Test-only: makes the active file take `bytes` more bytes and then fail one write, as a
	/// full disk does (see [`VLogWriter::fail_writes_after`]).
	#[cfg(test)]
	pub(crate) fn fail_writes_after(&self, bytes: usize) {
		self.writer.write().as_mut().expect("a file was written to").fail_writes_after(bytes);
	}

	/// Registers a VLog file in the files map for tracking
	fn register_vlog_file(&self, file_id: u32, path: PathBuf, size: u64) {
		let mut files_map = self.files_map.write();
		files_map.insert(file_id, Arc::new(VLogFile::new(file_id, path, size)));
	}

	pub(crate) fn close(&self) -> Result<()> {
		self.sync()?;
		Ok(())
	}

	/// Makes every file of the log durable and then lets go of every handle the log holds on
	/// them: the writer's, the queue of files that wait for an fsync, and the handles that reads
	/// were served through. A file system that keeps one handle per path (a browser's) cannot
	/// open the same files again, or remove or rename them, while any of them is held, and the
	/// handles of a log that is closed are held until the log itself is dropped.
	///
	/// The caller has stopped appending: this is for the end of the life of the log, after the
	/// last append (a flush at shutdown appends too, so it comes before this). The files stay in
	/// the registry, and a later read opens its file again. It is not what [`close`](Self::close)
	/// does, which is what a log that is not released goes on to be used after.
	#[allow(dead_code)] // The engine's close calls it once the engine has been moved over (M8).
	pub(crate) fn release_files(&self) -> Result<()> {
		self.sync()?;
		let mut writer = self.writer.write();
		if let Some(writer) = writer.as_mut() {
			writer.flush()?;
		}
		*writer = None;
		self.unsynced.lock().clear();
		self.file_handles.write().clear();
		Ok(())
	}
}
