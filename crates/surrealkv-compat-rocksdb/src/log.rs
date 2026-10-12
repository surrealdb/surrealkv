//! RocksDB's log format, used by the write-ahead log and by the MANIFEST: 32 KiB blocks holding
//! physical records `crc(4) length(2) type(1) [log number(4)] payload`, which carry logical records
//! as a full record or as first, middle and last fragments.

use std::io::Read;

use crate::coding::{crc32c, unmask_crc};
use crate::error::{corrupt, unsupported, Result};

const BLOCK_SIZE: usize = 32768;
const HEADER_SIZE: usize = 7;
const RECYCLABLE_HEADER_SIZE: usize = 11;

const ZERO: u8 = 0;
const FULL: u8 = 1;
const FIRST: u8 = 2;
const MIDDLE: u8 = 3;
const LAST: u8 = 4;
const RECYCLABLE_FULL: u8 = 5;
const RECYCLABLE_FIRST: u8 = 6;
const RECYCLABLE_MIDDLE: u8 = 7;
const RECYCLABLE_LAST: u8 = 8;
const SET_COMPRESSION: u8 = 9;
const UDT_SIZE: u8 = 10;
const RECYCLABLE_UDT_SIZE: u8 = 11;
/// Types with this bit that a reader does not know are ignored (the predecessor WAL info is one).
const SAFE_IGNORE_MASK: u8 = 1 << 7;

/// What a logical record holds.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RecordKind {
	/// A write batch (WAL) or a version edit (MANIFEST).
	Data,
	/// The sizes of the user-defined timestamps of the column families: `(cf id u32, size u16)`
	/// pairs. Written ahead of the first batch that needs them.
	UdtSizes,
}

pub(crate) struct Record {
	pub(crate) kind: RecordKind,
	pub(crate) payload: Vec<u8>,
}

/// How reading ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum End {
	/// A clean end of the log.
	Clean,
	/// The data stops inside a record: a write that never finished, which is what a crash leaves.
	/// RocksDB opens a MANIFEST like that and ignores the unfinished edit.
	Truncated,
	/// A record that is there but wrong: a checksum that does not match, or a length that cannot
	/// be.
	Damaged,
}

pub(crate) struct LogReader<R: Read> {
	file: R,
	name: String,
	/// The number of the log file, which the records of a recycled file carry in their headers.
	log_number: Option<u64>,
	block: Vec<u8>,
	pos: usize,
	len: usize,
	eof: bool,
	/// Where the damage was, once found.
	pub(crate) end: End,
}

impl<R: Read> LogReader<R> {
	pub(crate) fn new(file: R, name: impl Into<String>) -> Self {
		Self::with_log_number(file, name, None)
	}

	/// A reader of the log file with this number. The records of a recycled log file that belong
	/// to the file's earlier life carry another number, and RocksDB takes the first of them as
	/// the end of the log.
	pub(crate) fn with_log_number(
		file: R,
		name: impl Into<String>,
		log_number: Option<u64>,
	) -> Self {
		Self {
			file,
			name: name.into(),
			log_number,
			block: vec![0; BLOCK_SIZE],
			pos: 0,
			len: 0,
			eof: false,
			end: End::Clean,
		}
	}

	/// Refills the block buffer. Returns false at the end of the file.
	fn fill(&mut self) -> Result<bool> {
		if self.eof {
			return Ok(false);
		}
		let mut filled = 0;
		while filled < BLOCK_SIZE {
			let n = self.file.read(&mut self.block[filled..])?;
			if n == 0 {
				self.eof = true;
				break;
			}
			filled += n;
		}
		self.pos = 0;
		self.len = filled;
		Ok(filled > 0)
	}

	/// The next physical record: `(type, payload)`, or `None` at the end or at damage.
	fn physical(&mut self) -> Result<Option<(u8, Vec<u8>)>> {
		loop {
			if self.len - self.pos < HEADER_SIZE {
				// The trailer of a full block, too small for a header, is padding. Bytes left at
				// the end of the last, short block are the start of a record that was never
				// finished.
				if self.pos < self.len && self.len < BLOCK_SIZE {
					self.end = End::Truncated;
					return Ok(None);
				}
				if !self.fill()? {
					return Ok(None);
				}
				continue;
			}
			let header = &self.block[self.pos..self.pos + HEADER_SIZE];
			let stored = u32::from_le_bytes(header[0..4].try_into().expect("4"));
			let length = u16::from_le_bytes(header[4..6].try_into().expect("2")) as usize;
			let kind = header[6];

			if kind == ZERO && length == 0 && stored == 0 {
				// Preallocated, zero-filled space: nothing follows in this block.
				self.pos = self.len;
				continue;
			}
			let recyclable = matches!(
				kind,
				RECYCLABLE_FULL
					| RECYCLABLE_FIRST
					| RECYCLABLE_MIDDLE
					| RECYCLABLE_LAST
					| RECYCLABLE_UDT_SIZE
					| 131
			);
			let header_size = if recyclable {
				RECYCLABLE_HEADER_SIZE
			} else {
				HEADER_SIZE
			};
			if recyclable {
				if let Some(expected) = self.log_number {
					let number = u32::from_le_bytes(
						self.block[self.pos + 7..self.pos + 11].try_into().expect("4"),
					);
					if u64::from(number) != expected {
						// A record of the file's earlier life: the log ends here.
						self.eof = true;
						self.pos = self.len;
						return Ok(None);
					}
				}
			}
			if self.pos + header_size + length > self.len {
				// The record runs past the data there is. In the last, short block that is a
				// write that never finished; in a full block no record can be that long.
				self.end = if self.len < BLOCK_SIZE {
					End::Truncated
				} else {
					End::Damaged
				};
				return Ok(None);
			}
			let covered = &self.block[self.pos + 6..self.pos + header_size + length];
			let expected = unmask_crc(stored);
			let actual = crc32c(covered);
			if expected != actual {
				self.end = End::Damaged;
				return Ok(None);
			}
			let payload =
				self.block[self.pos + header_size..self.pos + header_size + length].to_vec();
			self.pos += header_size + length;
			return Ok(Some((kind, payload)));
		}
	}

	/// The next logical record, or `None` at the end of the log (check [`Self::end`] for damage).
	pub(crate) fn next_record(&mut self) -> Result<Option<Record>> {
		let mut fragments: Option<Vec<u8>> = None;
		loop {
			let Some((kind, payload)) = self.physical()? else {
				// A record that was being assembled and never finished is a torn write.
				if fragments.is_some() && self.end == End::Clean {
					self.end = End::Truncated;
				}
				return Ok(None);
			};
			match kind {
				FULL | RECYCLABLE_FULL => {
					if fragments.is_some() {
						self.end = End::Damaged;
						return Ok(None);
					}
					return Ok(Some(Record {
						kind: RecordKind::Data,
						payload,
					}));
				}
				FIRST | RECYCLABLE_FIRST => {
					if fragments.is_some() {
						self.end = End::Damaged;
						return Ok(None);
					}
					fragments = Some(payload);
				}
				MIDDLE | RECYCLABLE_MIDDLE => match fragments.as_mut() {
					Some(buf) => buf.extend_from_slice(&payload),
					None => {
						self.end = End::Damaged;
						return Ok(None);
					}
				},
				LAST | RECYCLABLE_LAST => match fragments.take() {
					Some(mut buf) => {
						buf.extend_from_slice(&payload);
						return Ok(Some(Record {
							kind: RecordKind::Data,
							payload: buf,
						}));
					}
					None => {
						self.end = End::Damaged;
						return Ok(None);
					}
				},
				SET_COMPRESSION => {
					if payload.first().copied().unwrap_or(0) != 0 {
						return Err(unsupported(format!(
							"{} is written with a compressed log, which this reader does not decode",
							self.name
						)));
					}
				}
				UDT_SIZE | RECYCLABLE_UDT_SIZE => {
					return Ok(Some(Record {
						kind: RecordKind::UdtSizes,
						payload,
					}));
				}
				other if other & SAFE_IGNORE_MASK != 0 => {}
				other => {
					return Err(corrupt(format!(
						"{} holds a record of unknown type {other}",
						self.name
					)))
				}
			}
		}
	}
}

/// A writer of the log format for the tests of the readers that sit on it.
#[cfg(test)]
pub(crate) mod testing {
	use super::*;
	use crate::coding::{crc32c, mask_crc};

	fn emit(out: &mut Vec<u8>, kind: u8, recycle: Option<u32>, payload: &[u8]) {
		let mut covered = vec![kind];
		if let Some(number) = recycle {
			covered.extend_from_slice(&number.to_le_bytes());
		}
		covered.extend_from_slice(payload);
		out.extend_from_slice(&mask_crc(crc32c(&covered)).to_le_bytes());
		out.extend_from_slice(&(payload.len() as u16).to_le_bytes());
		out.extend_from_slice(&covered);
	}

	/// `records` as a log, split over 32 KiB blocks the way RocksDB writes them. With `recycle`
	/// the records are of the recyclable kind and carry that log number.
	pub(crate) fn log_bytes(records: &[Vec<u8>], recycle: Option<u32>) -> Vec<u8> {
		let header = if recycle.is_some() {
			RECYCLABLE_HEADER_SIZE
		} else {
			HEADER_SIZE
		};
		let mut out = Vec::new();
		for record in records {
			let mut rest: &[u8] = record;
			let mut first = true;
			loop {
				let left = BLOCK_SIZE - out.len() % BLOCK_SIZE;
				if left < header {
					out.extend(std::iter::repeat_n(0u8, left));
					continue;
				}
				let take = rest.len().min(left - header);
				let last = take == rest.len();
				let kind = match (first, last, recycle.is_some()) {
					(true, true, false) => FULL,
					(true, false, false) => FIRST,
					(false, false, false) => MIDDLE,
					(false, true, false) => LAST,
					(true, true, true) => RECYCLABLE_FULL,
					(true, false, true) => RECYCLABLE_FIRST,
					(false, false, true) => RECYCLABLE_MIDDLE,
					(false, true, true) => RECYCLABLE_LAST,
				};
				emit(&mut out, kind, recycle, &rest[..take]);
				rest = &rest[take..];
				first = false;
				if last {
					break;
				}
			}
		}
		out
	}

	/// A write batch of puts (and deletes, with `None`) from `sequence` on, for the default column
	/// family. Keys and values must be shorter than 128 bytes.
	pub(crate) fn batch(sequence: u64, writes: &[(&[u8], Option<&[u8]>)]) -> Vec<u8> {
		let mut out = sequence.to_le_bytes().to_vec();
		out.extend_from_slice(&(writes.len() as u32).to_le_bytes());
		for (key, value) in writes {
			match value {
				Some(value) => {
					out.push(1);
					out.push(key.len() as u8);
					out.extend_from_slice(key);
					out.push(value.len() as u8);
					out.extend_from_slice(value);
				}
				None => {
					out.push(0);
					out.push(key.len() as u8);
					out.extend_from_slice(key);
				}
			}
		}
		out
	}
}

#[cfg(test)]
mod tests {
	use super::testing::log_bytes;
	use super::*;

	fn read_all(bytes: &[u8], log_number: Option<u64>) -> (Vec<Vec<u8>>, End) {
		let mut reader = LogReader::with_log_number(bytes, "test.log", log_number);
		let mut out = Vec::new();
		while let Some(record) = reader.next_record().unwrap() {
			out.push(record.payload);
		}
		(out, reader.end)
	}

	fn payloads() -> Vec<Vec<u8>> {
		vec![b"one".to_vec(), vec![7u8; 100], b"three".to_vec()]
	}

	#[test]
	fn a_log_reads_back_whole() {
		let (got, end) = read_all(&log_bytes(&payloads(), None), None);
		assert_eq!(got, payloads());
		assert_eq!(end, End::Clean);
	}

	#[test]
	fn a_record_longer_than_a_block_is_put_back_together() {
		let mut records = payloads();
		records.insert(1, (0..70_000u32).map(|i| (i % 251) as u8).collect());
		let (got, end) = read_all(&log_bytes(&records, None), None);
		assert_eq!(got, records);
		assert_eq!(end, End::Clean);
	}

	#[test]
	fn a_torn_last_record_is_the_end_of_the_log_not_damage() {
		let bytes = log_bytes(&payloads(), None);
		// Every cut inside the last record leaves the first two intact.
		let last_start = bytes.len() - (HEADER_SIZE + b"three".len());
		for cut in last_start + 1..bytes.len() {
			let (got, end) = read_all(&bytes[..cut], None);
			assert_eq!(got, payloads()[..2].to_vec(), "cut at {cut}");
			assert_eq!(end, End::Truncated, "cut at {cut}");
		}
	}

	#[test]
	fn a_record_that_is_there_and_wrong_is_damage() {
		let mut bytes = log_bytes(&payloads(), None);
		let flipped = HEADER_SIZE + 3 + HEADER_SIZE + 10;
		bytes[flipped] ^= 0x40;
		let (got, end) = read_all(&bytes, None);
		assert_eq!(got, payloads()[..1].to_vec());
		assert_eq!(end, End::Damaged);
	}

	#[test]
	fn a_first_fragment_with_nothing_after_it_is_a_torn_write() {
		let big: Vec<u8> = vec![9u8; 40_000];
		let bytes = log_bytes(&[b"head".to_vec(), big], None);
		// Keep the first block, which ends in the first fragment of the big record.
		let (got, end) = read_all(&bytes[..BLOCK_SIZE], None);
		assert_eq!(got, vec![b"head".to_vec()]);
		assert_eq!(end, End::Truncated);
	}

	#[test]
	fn a_recycled_log_ends_where_the_files_earlier_life_begins() {
		// The file was a log numbered 7; it is log 9 now and was overwritten as far as the second
		// record. What follows is the old log, whose records carry 7.
		let new = log_bytes(&payloads()[..2], Some(9));
		let old = log_bytes(&[b"stale".to_vec(), b"staler".to_vec()], Some(7));
		let mut bytes = new.clone();
		bytes.extend_from_slice(&old);
		let (got, end) = read_all(&bytes, Some(9));
		assert_eq!(got, payloads()[..2].to_vec());
		assert_eq!(end, End::Clean);
		// Without the number the stale records would be taken for the log's own.
		let (got, _) = read_all(&bytes, None);
		assert_eq!(got.len(), 4);
	}
}
