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

//! WAL format constants and types.
//!
//! Defines the on-disk format for the Write-Ahead Log (WAL).
//!
//! # File Format
//!
//! A WAL file is broken down into 32KB blocks. Each block contains one or more records.
//! If a record doesn't fit in the remaining space of a block, it's fragmented across
//! multiple blocks.
//!
//! ```text
//! File Layout:
//!   +-----+-------------+--+----+----------+------+-- ... ----+
//!   | r0  |     r1      |P | r2 |    r3    |  r4  |           |
//!   +-----+-------------+--+----+----------+------+-- ... ----+
//!   <--- kBlockSize ------>|<-- kBlockSize ------>|
//! ```
//!
//! Where:
//! - rN = variable size records
//! - P = Padding (zeros)
//!
//! # Record Format
//!
//! ## Record Format (7 bytes header):
//! ```text
//! +---------+-----------+-----------+--- ... ---+
//! |CRC (4B) | Size (2B) | Type (1B) | Payload   |
//! +---------+-----------+-----------+--- ... ---+
//! ```
//!
//! Where:
//! - CRC = 32-bit CRC computed over the record type and payload
//! - Size = Length of the payload data (little-endian)
//! - Type = Type of record (see RecordType enum)
//! - Payload = Byte stream of the specified size

use std::io;

use super::segment::{IOError, Result};

/// The size of a single block in bytes (32KB).
///
/// Records are written in blocks of this size. If a record doesn't fit in the
/// remaining space of a block, it will be fragmented across multiple blocks.
pub const BLOCK_SIZE: usize = 32 * 1024;

/// Length of the record header in bytes.
///
/// Header format: CRC (4 bytes) + Length (2 bytes) + Type (1 byte) = 7 bytes
pub const HEADER_SIZE: usize = 7;

/// Enum representing different types of records in the Write-Ahead Log.
///
/// Records can be:
/// - Full records that fit entirely in one block
/// - Fragmented records split across multiple blocks (First, Middle, Last)
/// - Metadata records for compression and other settings
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum RecordType {
	/// Indicates that the rest of the block is empty (zero-filled padding).
	Empty = 0,

	/// A complete record that fits entirely within a single block.
	Full = 1,

	/// The first fragment of a record that spans multiple blocks.
	First = 2,

	/// A middle fragment of a record (not first, not last).
	Middle = 3,

	/// The final fragment of a record.
	Last = 4,

	// Metadata record types
	/// Indicates the compression type used for subsequent records.
	SetCompressionType = 9,
}

impl RecordType {
	/// Converts a u8 value to a RecordType.
	///
	/// # Errors
	///
	/// Returns an error if the value doesn't correspond to a valid RecordType.
	pub fn from_u8(value: u8) -> Result<Self> {
		match value {
			0 => Ok(RecordType::Empty),
			1 => Ok(RecordType::Full),
			2 => Ok(RecordType::First),
			3 => Ok(RecordType::Middle),
			4 => Ok(RecordType::Last),
			9 => Ok(RecordType::SetCompressionType),
			_ => Err(super::segment::Error::IO(IOError::new(
				io::ErrorKind::InvalidInput,
				"Invalid Record Type",
			))),
		}
	}
}

/// Compression types supported by the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
	/// No compression.
	None = 0,

	/// LZ4 compression using lz4_flex.
	Lz4 = 1,
}

impl CompressionType {
	/// Converts a u8 value to a CompressionType.
	pub fn from_u8(value: u8) -> Result<Self> {
		match value {
			0 => Ok(CompressionType::None),
			1 => Ok(CompressionType::Lz4),
			_ => Err(super::segment::Error::IO(IOError::new(
				io::ErrorKind::InvalidInput,
				"Invalid Compression Type",
			))),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_record_type_from_u8() {
		assert_eq!(RecordType::from_u8(0).unwrap(), RecordType::Empty);
		assert_eq!(RecordType::from_u8(1).unwrap(), RecordType::Full);
		assert_eq!(RecordType::from_u8(9).unwrap(), RecordType::SetCompressionType);
		assert!(RecordType::from_u8(5).is_err()); // Recyclable types no longer supported
		assert!(RecordType::from_u8(255).is_err());
	}

	#[test]
	fn test_compression_type_from_u8() {
		assert_eq!(CompressionType::from_u8(0).unwrap(), CompressionType::None);
		assert!(CompressionType::from_u8(255).is_err());
	}
}
