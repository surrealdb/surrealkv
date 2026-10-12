//! The write-ahead log of an LSMV1 database: a frozen copy of the read side of v0.21.4's
//! `wal/reader.rs`, `wal/mod.rs` and `wal/recovery.rs`, which are the same in v0.20.0 to v0.21.4.
//!
//! A segment is a sequence of blocks of 32 KiB, which hold records and are padded with zeros
//! where the next record does not fit:
//!
//! ```text
//! CRC-32 (u32) | payload length (u16) | type (u8) | payload      all big endian
//! ```
//!
//! The CRC covers the type and the payload. A record that does not fit in what is left of its
//! block is split across blocks as a first, any number of middle, and a last fragment; one that
//! does is full. A record of type 9 is not data: it sets the compression of the records after it
//! (its payload is one byte, 0 none and 1 LZ4 with the size prepended).
//!
//! What v0.x did with what it read, which is what [`read_segment`] does too: it stopped reading
//! a segment at its first damaged record, kept what came before and went on with the next
//! segment; and a segment that ends in the middle of a record, as a crash leaves it, ended there
//! without damage.

use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;

use crate::{batch, Error, Result, WalRead, WalSegment};

const BLOCK_SIZE: usize = 32 * 1024;
const HEADER_SIZE: usize = 7;

const EMPTY: u8 = 0;
const FULL: u8 = 1;
const FIRST: u8 = 2;
const MIDDLE: u8 = 3;
const LAST: u8 = 4;
const SET_COMPRESSION_TYPE: u8 = 9;

pub(crate) fn segments(dir: &Path) -> Result<Vec<WalSegment>> {
	let entries = match fs::read_dir(dir) {
		Ok(entries) => entries,
		Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
		Err(e) => return Err(e.into()),
	};
	let mut segments = Vec::new();
	for entry in entries {
		let entry = entry?;
		if !entry.file_type()?.is_file() {
			continue;
		}
		let name = entry.file_name();
		let Some(id) = name.to_str().and_then(|name| name.strip_suffix(".wal")) else {
			continue;
		};
		// `<id:020>.wal`, which is what v0.x wrote; anything else in the directory is not a
		// segment.
		if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
			continue;
		}
		let Ok(id) = id.parse::<u64>() else {
			continue;
		};
		segments.push(WalSegment {
			id,
			path: entry.path(),
		});
	}
	segments.sort_by_key(|segment| segment.id);
	Ok(segments)
}

/// Why reading the next record failed.
enum Stop {
	/// The segment ended: after a record, or in the middle of one.
	End,
	/// A record is damaged.
	Damage(String),
	/// The file could not be read.
	Io(io::Error),
}

impl From<io::Error> for Stop {
	fn from(e: io::Error) -> Self {
		Stop::Io(e)
	}
}

fn damaged<T>(reason: &str) -> std::result::Result<T, Stop> {
	Err(Stop::Damage(reason.to_string()))
}

struct Reader {
	file: File,
	/// The block being read.
	buffer: Vec<u8>,
	offset: usize,
	/// The file has no more blocks after the one in `buffer`.
	eof: bool,
	lz4: bool,
	/// The offset of the block in the file, for what is said about damage.
	block_start: u64,
}

impl Reader {
	/// Reads the next block, dropping what is left of this one. `false` at the end of the file.
	fn read_more(&mut self) -> io::Result<bool> {
		if self.eof {
			return Ok(false);
		}
		self.block_start += self.buffer.len() as u64;
		self.buffer.clear();
		self.buffer.resize(BLOCK_SIZE, 0);
		let mut filled = 0;
		while filled < BLOCK_SIZE {
			match self.file.read(&mut self.buffer[filled..]) {
				Ok(0) => break,
				Ok(n) => filled += n,
				Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
				Err(e) => return Err(e),
			}
		}
		self.buffer.truncate(filled);
		self.offset = 0;
		if filled < BLOCK_SIZE {
			self.eof = true;
		}
		Ok(filled > 0)
	}

	fn remaining(&self) -> usize {
		self.buffer.len() - self.offset
	}

	/// A fragment that claims more than the block holds. In the last block of a file that ends
	/// short, which is where a crash leaves a write that did not finish, that is the end of the
	/// log; anywhere else the length is damaged.
	fn overrun(&self, what: &str) -> Stop {
		if self.eof && self.buffer.len() < BLOCK_SIZE {
			Stop::End
		} else {
			Stop::Damage(format!(
				"{what} at offset {} runs past its block",
				self.block_start + self.offset as u64
			))
		}
	}

	fn next(&mut self) -> std::result::Result<Vec<u8>, Stop> {
		let mut record = Vec::new();
		let mut fragments = 0;
		loop {
			if self.remaining() < HEADER_SIZE {
				// What is left of a block that cannot hold a header is padding.
				if !self.read_more()? {
					return Err(Stop::End);
				}
				continue;
			}
			let header = &self.buffer[self.offset..self.offset + HEADER_SIZE];
			let crc = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
			let length = usize::from(u16::from_be_bytes([header[4], header[5]]));
			let kind = header[6];
			self.offset += HEADER_SIZE;

			match kind {
				EMPTY => {
					// The rest of the block is padding.
					if self.buffer[self.offset..].iter().any(|b| *b != 0) {
						return damaged("a non-zero byte in the padding of a block");
					}
					self.offset = self.buffer.len();
					continue;
				}
				SET_COMPRESSION_TYPE => {
					if length > self.remaining() {
						return Err(self.overrun("a compression record"));
					}
					if length > 0 {
						let byte = self.buffer[self.offset];
						self.offset += length;
						self.lz4 = match byte {
							0 => false,
							1 => true,
							other => {
								return Err(Stop::Damage(format!(
									"a compression record with the unknown type {other}"
								)))
							}
						};
					}
					continue;
				}
				FULL | FIRST if fragments != 0 => {
					return damaged("a record that starts inside another");
				}
				MIDDLE | LAST if fragments == 0 => {
					return damaged("a fragment of a record whose start is missing");
				}
				FULL | FIRST | MIDDLE | LAST => {}
				_ => return damaged("a record of an unknown type"),
			}

			if length > self.remaining() {
				return Err(self.overrun("a record"));
			}
			let payload = &self.buffer[self.offset..self.offset + length];
			let mut hasher = crc32fast::Hasher::new();
			hasher.update(&[kind]);
			hasher.update(payload);
			if hasher.finalize() != crc {
				return damaged("a record whose checksum does not match");
			}
			record.extend_from_slice(payload);
			self.offset += length;
			if kind == LAST || kind == FULL {
				break;
			}
			fragments += 1;
		}

		if self.lz4 && !record.is_empty() {
			record = lz4_flex::decompress_size_prepended(&record)
				.map_err(|e| Stop::Damage(format!("a record that does not decompress: {e}")))?;
		}
		Ok(record)
	}
}

pub(crate) fn read_segment(path: &Path) -> Result<WalRead> {
	let mut reader = Reader {
		file: File::open(path)?,
		buffer: Vec::new(),
		offset: 0,
		eof: false,
		lz4: false,
		block_start: 0,
	};
	let mut batches = Vec::new();
	let damage = loop {
		match reader.next() {
			Ok(record) => batches.push(batch::decode(&record)?),
			Err(Stop::End) => break None,
			Err(Stop::Damage(reason)) => break Some(reason),
			Err(Stop::Io(e)) => return Err(Error::Io(e)),
		}
	};
	Ok(WalRead {
		batches,
		damage,
	})
}

#[cfg(test)]
pub(crate) mod testing {
	use super::*;

	/// Writes records the way v0.21.4's writer did: `(compress, payload)` per record, with the
	/// compression record in front of the first and the fragments and the padding of blocks.
	pub(crate) fn segment(records: &[Vec<u8>], lz4: bool) -> Vec<u8> {
		let mut out = Vec::new();
		let emit = |out: &mut Vec<u8>, kind: u8, payload: &[u8]| {
			let left = BLOCK_SIZE - out.len() % BLOCK_SIZE;
			if left < HEADER_SIZE {
				out.extend(std::iter::repeat_n(0u8, left));
			}
			let mut hasher = crc32fast::Hasher::new();
			hasher.update(&[kind]);
			hasher.update(payload);
			out.extend_from_slice(&hasher.finalize().to_be_bytes());
			out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
			out.push(kind);
			out.extend_from_slice(payload);
		};
		if lz4 {
			emit(&mut out, SET_COMPRESSION_TYPE, &[1]);
		}
		for record in records {
			let stored = if lz4 {
				lz4_flex::compress_prepend_size(record)
			} else {
				record.clone()
			};
			let mut rest = stored.as_slice();
			let mut first = true;
			loop {
				let left = BLOCK_SIZE - out.len() % BLOCK_SIZE;
				let left = if left < HEADER_SIZE {
					BLOCK_SIZE - HEADER_SIZE
				} else {
					left - HEADER_SIZE
				};
				if rest.len() <= left {
					emit(
						&mut out,
						if first {
							FULL
						} else {
							LAST
						},
						rest,
					);
					break;
				}
				emit(
					&mut out,
					if first {
						FIRST
					} else {
						MIDDLE
					},
					&rest[..left],
				);
				rest = &rest[left..];
				first = false;
			}
		}
		out
	}
}

#[cfg(test)]
mod tests {
	use super::testing::segment;
	use super::*;
	use crate::batch::testing::encode;

	fn write(dir: &Path, name: &str, bytes: &[u8]) -> std::path::PathBuf {
		let path = dir.join(name);
		fs::write(&path, bytes).unwrap();
		path
	}

	fn batches(n: u64) -> Vec<Vec<u8>> {
		(0..n)
			.map(|i| {
				let key = format!("key{i}");
				let value = vec![i as u8; 5000 * (i as usize % 3)];
				let mut stored = vec![0, 1];
				stored.extend_from_slice(&value);
				encode(i * 10 + 1, &[(2, key.as_bytes(), Some(&stored), i)])
			})
			.collect()
	}

	#[test]
	fn reads_records_that_span_blocks_with_and_without_compression() {
		let dir = tempfile::tempdir().unwrap();
		for lz4 in [false, true] {
			let records = batches(40);
			let path = write(dir.path(), "a.wal", &segment(&records, lz4));
			let read = read_segment(&path).unwrap();
			assert_eq!(read.damage, None, "lz4 {lz4}");
			assert_eq!(read.batches.len(), 40);
			for (i, batch) in read.batches.iter().enumerate() {
				assert_eq!(batch.starting_seq, i as u64 * 10 + 1);
				assert_eq!(batch.entries[0].key, format!("key{i}").as_bytes());
			}
		}
	}

	#[test]
	fn a_segment_cut_in_the_middle_of_a_record_is_not_damaged() {
		let dir = tempfile::tempdir().unwrap();
		let records = batches(12);
		let bytes = segment(&records, false);
		let whole = read_segment(&write(dir.path(), "w.wal", &bytes)).unwrap();
		assert_eq!(whole.batches.len(), 12);
		let mut last = 0;
		for cut in (0..bytes.len()).step_by(997).chain([bytes.len() - 1, bytes.len() - 7]) {
			let read = read_segment(&write(dir.path(), "c.wal", &bytes[..cut])).unwrap();
			assert_eq!(read.damage, None, "cut at {cut}");
			assert!(read.batches.len() <= whole.batches.len());
			assert_eq!(read.batches[..], whole.batches[..read.batches.len()], "cut at {cut}");
			last = last.max(read.batches.len());
		}
		assert!(last > 5);
	}

	#[test]
	fn a_damaged_record_stops_the_segment_and_is_reported() {
		let dir = tempfile::tempdir().unwrap();
		let records = batches(12);
		let mut bytes = segment(&records, false);
		let intact = read_segment(&write(dir.path(), "i.wal", &bytes)).unwrap();
		// Damage the payload of the fourth record.
		let mut offset = 0;
		for record in &records[..3] {
			offset += HEADER_SIZE + record.len();
		}
		bytes[offset + HEADER_SIZE + 3] ^= 0xff;
		let read = read_segment(&write(dir.path(), "d.wal", &bytes)).unwrap();
		assert_eq!(read.batches.len(), 3);
		assert_eq!(read.batches[..], intact.batches[..3]);
		assert!(read.damage.unwrap().contains("checksum"));
	}

	#[test]
	fn lists_segments_oldest_first_and_ignores_what_is_not_one() {
		let dir = tempfile::tempdir().unwrap();
		assert!(segments(&dir.path().join("missing")).unwrap().is_empty());
		for name in [
			"00000000000000000010.wal",
			"00000000000000000002.wal",
			"notes.txt",
			".DS_Store",
			"7.log",
		] {
			write(dir.path(), name, b"");
		}
		fs::create_dir(dir.path().join("00000000000000000099.wal")).unwrap();
		let ids: Vec<u64> = segments(dir.path()).unwrap().iter().map(|s| s.id).collect();
		assert_eq!(ids, [2, 10]);
	}
}
