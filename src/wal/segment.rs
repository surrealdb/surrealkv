//  Copyright (c) 2024 SurrealDB Ltd.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! WAL Segment Utilities

use std::fmt;
use std::fs::read_dir;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::PoisonError;

use crc32fast::Hasher;

use super::format::RecordType;

/// Generates a segment file name from an index and extension.
pub(crate) fn segment_name(index: u64, ext: &str) -> String {
	if ext.is_empty() {
		format!("{index:020}")
	} else {
		format!("{index:020}.{ext}")
	}
}

/// Parses a segment file name to extract the ID and extension.
fn parse_segment_name(name: &str) -> Result<(u64, Option<String>)> {
	if let Some(dot_pos) = name.find('.') {
		let (id_part, ext_part) = name.split_at(dot_pos);
		let extension = ext_part.trim_start_matches('.');

		let index = id_part.parse::<u64>().map_err(|_| {
			Error::IO(IOError::new(io::ErrorKind::InvalidInput, "Invalid segment name format"))
		})?;

		return Ok((index, Some(extension.to_string())));
	}

	let index = name.parse::<u64>().map_err(|_| {
		Error::IO(IOError::new(io::ErrorKind::InvalidInput, "Invalid segment name format"))
	})?;

	Ok((index, None))
}

/// Gets the range of segment IDs present in the specified directory.
pub(crate) fn get_segment_range(dir: &Path, allowed_extension: Option<&str>) -> Result<(u64, u64)> {
	let refs = list_segment_ids(dir, allowed_extension)?;
	if refs.is_empty() {
		return Ok((0, 0));
	}
	Ok((refs[0], refs[refs.len() - 1]))
}

/// Lists the segment IDs found in the specified directory.
pub(crate) fn list_segment_ids(dir: &Path, allowed_extension: Option<&str>) -> Result<Vec<u64>> {
	let mut refs: Vec<u64> = Vec::new();
	let entries = read_dir(dir)?;

	for entry in entries {
		let file = entry?;

		if std::fs::metadata(file.path())?.is_file() {
			let fn_name = file.file_name();
			let fn_str = fn_name.to_string_lossy();
			let (index, extension) = parse_segment_name(&fn_str)?;

			if !should_include_file(allowed_extension, extension) {
				continue;
			}

			refs.push(index);
		}
	}

	refs.sort();
	Ok(refs)
}

/// Helper function to check if a file should be included based on extension filtering.
fn should_include_file(allowed_extension: Option<&str>, file_extension: Option<String>) -> bool {
	match (&allowed_extension, &file_extension) {
		(None, Some(_)) => false,
		(Some(allowed), Some(ext)) if allowed != ext => false,
		(Some(_), None) => false,
		_ => true,
	}
}

#[derive(Debug)]
pub(crate) struct SegmentRef {
	pub file_path: PathBuf,
	pub id: u64,
}

impl SegmentRef {
	/// Creates a vector of SegmentRef instances by reading segments in the specified directory.
	pub(crate) fn read_segments_from_directory(
		directory_path: &Path,
		allowed_extension: Option<&str>,
	) -> Result<Vec<SegmentRef>> {
		let mut segment_refs = Vec::new();

		let files = read_dir(directory_path)?;
		for file in files {
			let entry = file?;
			if entry.file_type()?.is_file() {
				let file_path = entry.path();
				let fn_name = entry.file_name();
				let fn_str = fn_name.to_string_lossy();
				let (index, extension) = parse_segment_name(&fn_str)?;

				if !should_include_file(allowed_extension, extension) {
					continue;
				}

				let segment_ref = SegmentRef {
					file_path,
					id: index,
				};

				segment_refs.push(segment_ref);
			}
		}

		segment_refs.sort_by(|a, b| a.id.cmp(&b.id));
		Ok(segment_refs)
	}
}

/// Calculates CRC32 checksum over record type and data.
pub(crate) fn calculate_crc32(record_type: &[u8], data: &[u8]) -> u32 {
	let mut hasher = Hasher::new();
	hasher.update(record_type);
	hasher.update(data);
	hasher.finalize()
}

/// Validates that a record type is valid for its position in a sequence.
pub(crate) fn validate_record_type(record_type: &RecordType, i: usize) -> Result<()> {
	match record_type {
		RecordType::Full => {
			if i != 0 {
				return Err(Error::IO(IOError::new(
					io::ErrorKind::Other,
					"Unexpected full record: first record expected",
				)));
			}
		}
		RecordType::First => {
			if i != 0 {
				return Err(Error::IO(IOError::new(
					io::ErrorKind::Other,
					"Unexpected first record: no previous records expected",
				)));
			}
		}
		RecordType::Middle => {
			if i == 0 {
				return Err(Error::IO(IOError::new(
					io::ErrorKind::Other,
					"Unexpected middle record: missing previous records",
				)));
			}
		}
		RecordType::Last => {
			if i == 0 {
				return Err(Error::IO(IOError::new(
					io::ErrorKind::Other,
					"Unexpected last record: missing previous records",
				)));
			}
		}
		_ => {
			return Err(Error::IO(IOError::new(io::ErrorKind::Other, "Invalid record type")));
		}
	}

	Ok(())
}

// ===== Error Types =====

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
	Corruption(CorruptionError),
	SegmentClosed,
	EmptyBuffer,
	Eof(usize),
	IO(IOError),
	Poison(String),
	RecordTooLarge,
	SegmentNotFound,
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Error::Corruption(err) => write!(f, "Corruption error: {err}"),
			Error::SegmentClosed => write!(f, "Segment is closed"),
			Error::EmptyBuffer => write!(f, "Buffer is empty"),
			Error::IO(err) => write!(f, "IO error: {err}"),
			Error::Eof(n) => write!(f, "EOF error after reading {n} bytes"),
			Error::Poison(msg) => write!(f, "Lock Poison: {msg}"),
			Error::RecordTooLarge => {
				write!(f, "Record is too large to fit in a segment. Increase max segment size")
			}
			Error::SegmentNotFound => write!(f, "Segment not found"),
		}
	}
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
	fn from(e: io::Error) -> Error {
		Error::IO(IOError {
			kind: e.kind(),
			message: e.to_string(),
		})
	}
}

impl<T: Sized> From<PoisonError<T>> for Error {
	fn from(e: PoisonError<T>) -> Error {
		Error::Poison(e.to_string())
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct IOError {
	kind: io::ErrorKind,
	message: String,
}

impl IOError {
	pub(crate) fn new(kind: io::ErrorKind, message: &str) -> Self {
		IOError {
			kind,
			message: message.to_string(),
		}
	}

	pub(crate) fn kind(&self) -> io::ErrorKind {
		self.kind
	}
}

impl fmt::Display for IOError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "kind={}, message={}", self.kind, self.message)
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct CorruptionError {
	kind: io::ErrorKind,
	message: String,
	pub segment_id: u64,
	pub offset: u64,
}

impl CorruptionError {
	pub(crate) fn new(kind: io::ErrorKind, message: &str, segment_id: u64, offset: u64) -> Self {
		CorruptionError {
			kind,
			message: message.to_string(),
			segment_id,
			offset,
		}
	}
}

impl fmt::Display for CorruptionError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"kind={}, message={}, segment_id={}, offset={}",
			self.kind, self.message, self.segment_id, self.offset
		)
	}
}

impl std::error::Error for CorruptionError {}

#[cfg(test)]
mod tests {
	use super::*;
	use std::fs::File;
	use tempdir::TempDir;

	fn create_temp_directory() -> TempDir {
		TempDir::new("test").unwrap()
	}

	fn create_segment_file(dir: &Path, name: &str) {
		let file_path = dir.join(name);
		let mut file = File::create(file_path).unwrap();
		use std::io::Write;
		file.write_all(b"dummy content").unwrap();
	}

	#[test]
	fn segment_name_with_extension() {
		let index = 100;
		let ext = "log";
		let expected = format!("{index:020}.{ext}");
		assert_eq!(segment_name(index, ext), expected);
	}

	#[test]
	fn segment_name_without_extension() {
		let index = 100;
		let expected = format!("{index:020}");
		assert_eq!(segment_name(index, ""), expected);
	}

	#[test]
	fn segment_name_with_compound_extension() {
		let index = 5;
		let name = segment_name(index, "wal.repair");
		assert_eq!(name, "00000000000000000005.wal.repair");

		let (parsed_id, parsed_ext) = parse_segment_name(&name).unwrap();
		assert_eq!(parsed_id, index);
		assert_eq!(parsed_ext, Some("wal.repair".to_string()));
	}

	#[test]
	fn parse_segment_name_with_extension() {
		let name = "00000000000000000010.log";
		let result = parse_segment_name(name).unwrap();
		assert_eq!(result, (10, Some("log".to_string())));
	}

	#[test]
	fn parse_segment_name_without_extension() {
		let name = "00000000000000000010";
		let result = parse_segment_name(name).unwrap();
		assert_eq!(result, (10, None));
	}

	#[test]
	fn parse_segment_name_with_compound_extension() {
		let name = "00000000000000000010.wal.repair";
		let result = parse_segment_name(name).unwrap();
		assert_eq!(result, (10, Some("wal.repair".to_string())));
	}

	#[test]
	fn parse_segment_name_invalid_format() {
		let name = "invalid_name";
		let result = parse_segment_name(name);
		assert!(result.is_err());
	}

	#[test]
	fn segments_empty_directory() {
		let temp_dir = create_temp_directory();
		let dir = temp_dir.path().to_path_buf();

		let result = get_segment_range(&dir, Some("wal")).unwrap();
		assert_eq!(result, (0, 0));
	}

	#[test]
	fn segments_non_empty_directory() {
		let temp_dir = create_temp_directory();
		let dir = temp_dir.path().to_path_buf();

		create_segment_file(&dir, "00000000000000000001.log");
		create_segment_file(&dir, "00000000000000000003.log");
		create_segment_file(&dir, "00000000000000000002.log");
		create_segment_file(&dir, "00000000000000000004.log");

		let result = get_segment_range(&dir, Some("log")).unwrap();
		assert_eq!(result, (1, 4));
	}

	#[test]
	fn test_get_segment_range_with_compound_extension() {
		let temp_dir = create_temp_directory();
		let dir = temp_dir.path().to_path_buf();

		create_segment_file(&dir, "00000000000000000001.wal");
		create_segment_file(&dir, "00000000000000000002.wal");
		create_segment_file(&dir, "00000000000000000003.wal.repair");
		create_segment_file(&dir, "00000000000000000004.wal.repair");
		create_segment_file(&dir, "00000000000000000005.wal.repair");
		create_segment_file(&dir, "00000000000000000006.wal");

		let result = get_segment_range(&dir, Some("wal")).unwrap();
		assert_eq!(result, (1, 6));

		let result = get_segment_range(&dir, Some("wal.repair")).unwrap();
		assert_eq!(result, (3, 5));

		let wal_segments = list_segment_ids(&dir, Some("wal")).unwrap();
		assert_eq!(wal_segments, vec![1, 2, 6]);

		let repair_segments = list_segment_ids(&dir, Some("wal.repair")).unwrap();
		assert_eq!(repair_segments, vec![3, 4, 5]);
	}

	#[test]
	fn test_list_segment_ids() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let dir_path = temp_dir.path();

		create_segment_file(dir_path, &segment_name(1, ""));
		create_segment_file(dir_path, &segment_name(2, ""));
		create_segment_file(dir_path, &segment_name(10, ""));

		let segment_ids = list_segment_ids(dir_path, None).unwrap();
		assert_eq!(segment_ids, vec![1, 2, 10]);
	}

	#[test]
	fn test_list_segment_ids_with_extension_filter() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let dir_path = temp_dir.path();

		create_segment_file(dir_path, &segment_name(1, ""));
		create_segment_file(dir_path, &segment_name(2, "repair"));
		create_segment_file(dir_path, &segment_name(3, "tmp"));
		create_segment_file(dir_path, &segment_name(4, "repair"));

		let segment_ids_no_ext = list_segment_ids(dir_path, None).unwrap();
		assert_eq!(segment_ids_no_ext, vec![1]);

		let segment_ids_repair = list_segment_ids(dir_path, Some("repair")).unwrap();
		assert_eq!(segment_ids_repair, vec![2, 4]);

		let segment_ids_tmp = list_segment_ids(dir_path, Some("tmp")).unwrap();
		assert_eq!(segment_ids_tmp, vec![3]);
	}

	#[test]
	fn test_should_include_file_helper() {
		assert!(should_include_file(None, None));
		assert!(!should_include_file(None, Some("repair".to_string())));
		assert!(!should_include_file(None, Some("tmp".to_string())));

		assert!(!should_include_file(Some("repair"), None));
		assert!(should_include_file(Some("repair"), Some("repair".to_string())));
		assert!(!should_include_file(Some("repair"), Some("tmp".to_string())));
		assert!(!should_include_file(Some("tmp"), Some("repair".to_string())));
	}
}
