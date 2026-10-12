//! The write-ahead log: write batches in the log format. Replaying the logs that the MANIFEST does
//! not count as flushed gives the writes that never reached a table.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::coding::{fixed32, fixed64, length_prefixed, varint32};
use crate::error::{corrupt, unsupported, Result};
use crate::log::{End, LogReader, RecordKind};

// Write batch tags (db/dbformat.h, ValueType).
const DELETION: u8 = 0x0;
const VALUE: u8 = 0x1;
const MERGE: u8 = 0x2;
const LOG_DATA: u8 = 0x3;
const CF_DELETION: u8 = 0x4;
const CF_VALUE: u8 = 0x5;
const CF_MERGE: u8 = 0x6;
const SINGLE_DELETION: u8 = 0x7;
const CF_SINGLE_DELETION: u8 = 0x8;
const NOOP: u8 = 0xD;
const CF_RANGE_DELETION: u8 = 0xE;
const RANGE_DELETION: u8 = 0xF;

/// What a write did to a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalOp {
	Put,
	Delete,
}

/// One write of a batch. `key` carries the timestamp suffix if the column family has one.
pub(crate) struct WalRecord {
	pub(crate) seq: u64,
	pub(crate) op: WalOp,
	pub(crate) key: Vec<u8>,
	pub(crate) value: Vec<u8>,
}

/// Everything the log holds, in order, and how reading ended.
pub(crate) struct WalContents {
	pub(crate) records: Vec<WalRecord>,
	/// The sequence number of the first write, and the one the next write would have.
	pub(crate) first_sequence: Option<u64>,
	pub(crate) next_sequence: Option<u64>,
	pub(crate) end: End,
	/// The timestamp size the log declared for the default column family, if it did.
	pub(crate) declared_timestamp_size: Option<usize>,
}

fn decode_batch(batch: &[u8], name: &str, out: &mut Vec<WalRecord>) -> Result<()> {
	if batch.len() < 12 {
		return Err(corrupt(format!("{name} holds a write batch of {} bytes", batch.len())));
	}
	let mut pos = 0;
	let sequence = fixed64(batch, &mut pos)?;
	let count = fixed32(batch, &mut pos)?;
	let mut seq = sequence;
	let mut data_records = 0u32;
	while pos < batch.len() {
		let tag = batch[pos];
		pos += 1;
		let cf_variant = matches!(
			tag,
			CF_DELETION | CF_VALUE | CF_MERGE | CF_SINGLE_DELETION | CF_RANGE_DELETION
		);
		if cf_variant {
			let cf = varint32(batch, &mut pos)?;
			if cf != 0 {
				return Err(unsupported(format!(
					"{name} holds a write to column family {cf}; only the default one is migrated"
				)));
			}
		}
		match tag {
			DELETION | CF_DELETION => {
				let key = length_prefixed(batch, &mut pos)?.to_vec();
				out.push(WalRecord {
					seq,
					op: WalOp::Delete,
					key,
					value: Vec::new(),
				});
				seq += 1;
				data_records += 1;
			}
			VALUE | CF_VALUE => {
				let key = length_prefixed(batch, &mut pos)?.to_vec();
				let value = length_prefixed(batch, &mut pos)?.to_vec();
				out.push(WalRecord {
					seq,
					op: WalOp::Put,
					key,
					value,
				});
				seq += 1;
				data_records += 1;
			}
			LOG_DATA => {
				length_prefixed(batch, &mut pos)?;
			}
			NOOP => {}
			MERGE | CF_MERGE => return Err(unsupported(format!("{name} holds a merge operand"))),
			SINGLE_DELETION | CF_SINGLE_DELETION => {
				return Err(unsupported(format!("{name} holds a single delete")))
			}
			RANGE_DELETION | CF_RANGE_DELETION => {
				return Err(unsupported(format!("{name} holds a range delete")))
			}
			other => {
				return Err(unsupported(format!(
					"{name} holds a write batch record of type {other:#04x}, which this reader does not handle"
				)))
			}
		}
	}
	if data_records != count {
		return Err(corrupt(format!(
			"{name} holds a write batch that says it has {count} records and has {data_records}"
		)));
	}
	Ok(())
}

/// Reads one write-ahead log. A torn or damaged end is not an error: RocksDB recovers a log up to
/// the first damaged record (point-in-time recovery), and so does this.
pub(crate) fn read_log(path: &Path, log_number: u64) -> Result<WalContents> {
	let name = path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
	let mut reader = LogReader::with_log_number(
		BufReader::new(File::open(path)?),
		name.clone(),
		Some(log_number),
	);
	let mut records = Vec::new();
	let mut declared = None;
	let mut first_sequence = None;
	while let Some(record) = reader.next_record()? {
		match record.kind {
			RecordKind::UdtSizes => {
				let mut pos = 0;
				while pos < record.payload.len() {
					let cf = fixed32(&record.payload, &mut pos)?;
					let size = u16::from_le_bytes(
						record
							.payload
							.get(pos..pos + 2)
							.ok_or_else(|| corrupt("a timestamp size record is cut short"))?
							.try_into()
							.expect("2"),
					);
					pos += 2;
					if cf == 0 {
						declared = Some(usize::from(size));
					}
				}
			}
			RecordKind::Data => {
				decode_batch(&record.payload, &name, &mut records)?;
				if first_sequence.is_none() && record.payload.len() >= 8 {
					first_sequence =
						Some(u64::from_le_bytes(record.payload[..8].try_into().expect("8")));
				}
			}
		}
	}
	let next_sequence = records.last().map(|r| r.seq + 1);
	Ok(WalContents {
		records,
		first_sequence,
		next_sequence,
		end: reader.end,
		declared_timestamp_size: declared,
	})
}
