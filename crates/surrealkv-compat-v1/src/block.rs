//! Blocks of a table: the handle that points at one, the trailer that checks it, and the entries
//! in it. A frozen copy of the read side of v0.21.4's `sstable/block.rs` and `sstable/table.rs`.
//!
//! ```text
//! on disk:   [block data (size bytes)] [compression type (1)] [masked crc32 (4, little endian)]
//! the data:  [entries] [restart offset (4, LE)] ... [number of restarts (4, LE)]
//! an entry:  [shared (varint)] [unshared (varint)] [value length (varint)] [key suffix] [value]
//! ```
//!
//! The checksum is the CRC-32 (IEEE) of the data as stored and the compression byte, masked as
//! LevelDB does: rotated right by 15 bits plus `0xa282ead8`. A block is stored as it was
//! compressed, with snappy (1) or not (0).

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use crate::{varint, Error, Result};

pub(crate) const TRAILER_LEN: usize = 5;
const MASK_DELTA: u32 = 0xa282_ead8;

/// Where a block is in the file: the offset of its data and the size of its data (the trailer
/// follows it).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Handle {
	pub offset: u64,
	pub size: u64,
}

impl Handle {
	/// Decodes a handle (two varints) and the number of bytes it took.
	pub(crate) fn decode(src: &[u8]) -> Option<(Self, usize)> {
		let mut pos = 0;
		let offset = varint::read(src, &mut pos)?;
		let size = varint::read(src, &mut pos)?;
		Some((
			Self {
				offset,
				size,
			},
			pos,
		))
	}
}

fn unmask(masked: u32) -> u32 {
	masked.wrapping_sub(MASK_DELTA).rotate_left(15)
}

/// Reads the block at `handle`, checks its checksum and decompresses it.
pub(crate) fn read_block(file: &mut File, file_len: u64, handle: Handle) -> Result<Vec<u8>> {
	handle
		.size
		.checked_add(TRAILER_LEN as u64)
		.and_then(|n| n.checked_add(handle.offset))
		.filter(|end| *end <= file_len)
		.ok_or_else(|| {
			Error::Corrupted(format!(
				"a block of {} bytes at {} is outside a file of {file_len} bytes",
				handle.size, handle.offset
			))
		})?;
	let size = handle.size as usize;
	let mut buf = vec![0u8; size + TRAILER_LEN];
	file.seek(SeekFrom::Start(handle.offset))?;
	file.read_exact(&mut buf)?;

	let (data, trailer) = buf.split_at(size);
	let compression = trailer[0];
	let want = unmask(u32::from_le_bytes([trailer[1], trailer[2], trailer[3], trailer[4]]));
	let mut hasher = crc32fast::Hasher::new();
	hasher.update(data);
	hasher.update(&[compression]);
	if hasher.finalize() != want {
		return Err(Error::Corrupted(format!(
			"the checksum of the block at {} does not match",
			handle.offset
		)));
	}
	match compression {
		0 => {
			buf.truncate(size);
			Ok(buf)
		}
		1 => snap::raw::Decoder::new().decompress_vec(data).map_err(|e| {
			Error::Corrupted(format!("the block at {} does not decompress: {e}", handle.offset))
		}),
		other => Err(Error::Corrupted(format!(
			"the block at {} has the unknown compression type {other}",
			handle.offset
		))),
	}
}

/// The entries of a decompressed block, in the order they are stored in.
pub(crate) struct BlockEntries {
	data: Vec<u8>,
	/// Where the entries end and the restart array starts.
	end: usize,
	offset: usize,
	key: Vec<u8>,
	value: (usize, usize),
}

impl BlockEntries {
	pub(crate) fn new(data: Vec<u8>) -> Result<Self> {
		if data.len() < 4 {
			return Err(Error::Corrupted(format!("a block of {} bytes", data.len())));
		}
		let n = data.len();
		let restarts = u32::from_le_bytes([data[n - 4], data[n - 3], data[n - 2], data[n - 1]]);
		let end = (restarts as usize)
			.checked_add(1)
			.and_then(|r| r.checked_mul(4))
			.and_then(|bytes| n.checked_sub(bytes))
			.ok_or_else(|| {
				Error::Corrupted(format!("a block of {n} bytes claims {restarts} restart points"))
			})?;
		Ok(Self {
			data,
			end,
			offset: 0,
			key: Vec::new(),
			value: (0, 0),
		})
	}

	/// Moves to the next entry. `false` at the end of the block.
	pub(crate) fn advance(&mut self) -> Result<bool> {
		if self.offset >= self.end {
			return Ok(false);
		}
		let corrupt = |what: &str| Error::Corrupted(format!("a block entry has {what}"));
		let mut pos = self.offset;
		let shared = varint::read(&self.data[..self.end], &mut pos)
			.ok_or_else(|| corrupt("a bad shared length"))?;
		let unshared = varint::read(&self.data[..self.end], &mut pos)
			.ok_or_else(|| corrupt("a bad key length"))?;
		let value_len = varint::read(&self.data[..self.end], &mut pos)
			.ok_or_else(|| corrupt("a bad value length"))?;
		let key_end = usize::try_from(unshared)
			.ok()
			.and_then(|n| pos.checked_add(n))
			.filter(|end| *end <= self.end)
			.ok_or_else(|| corrupt("a key that runs past the block"))?;
		let value_end = usize::try_from(value_len)
			.ok()
			.and_then(|n| key_end.checked_add(n))
			.filter(|end| *end <= self.end)
			.ok_or_else(|| corrupt("a value that runs past the block"))?;
		let shared = usize::try_from(shared)
			.ok()
			.filter(|shared| *shared <= self.key.len())
			.ok_or_else(|| corrupt("more shared bytes than the key before it has"))?;
		self.key.truncate(shared);
		self.key.extend_from_slice(&self.data[pos..key_end]);
		self.value = (key_end, value_end);
		self.offset = value_end;
		Ok(true)
	}

	pub(crate) fn key(&self) -> &[u8] {
		&self.key
	}

	pub(crate) fn value(&self) -> &[u8] {
		&self.data[self.value.0..self.value.1]
	}
}

#[cfg(test)]
pub(crate) mod testing {
	//! Builds blocks and tables the way v0.21.4 wrote them, for the tests of this crate.

	use super::*;

	pub(crate) fn mask(crc: u32) -> u32 {
		crc.rotate_right(15).wrapping_add(MASK_DELTA)
	}

	/// A block of the entries, with a restart point every `interval` entries.
	pub(crate) fn block(entries: &[(&[u8], &[u8])], interval: usize) -> Vec<u8> {
		let mut out = Vec::new();
		let mut restarts = vec![0u32];
		let mut last: Vec<u8> = Vec::new();
		let mut counter = 0;
		for (key, value) in entries {
			let mut shared = 0;
			if counter < interval {
				shared = last.iter().zip(key.iter()).take_while(|(a, b)| a == b).count();
			} else {
				restarts.push(out.len() as u32);
				counter = 0;
			}
			put_varint(&mut out, shared as u64);
			put_varint(&mut out, (key.len() - shared) as u64);
			put_varint(&mut out, value.len() as u64);
			out.extend_from_slice(&key[shared..]);
			out.extend_from_slice(value);
			last = key.to_vec();
			counter += 1;
		}
		for r in &restarts {
			out.extend_from_slice(&r.to_le_bytes());
		}
		out.extend_from_slice(&(restarts.len() as u32).to_le_bytes());
		out
	}

	pub(crate) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
		while value >= 0x80 {
			out.push((value as u8 & 0x7f) | 0x80);
			value >>= 7;
		}
		out.push(value as u8);
	}

	/// Appends the block to `file` as stored, compressed with snappy if `snappy`, and returns its
	/// handle.
	pub(crate) fn append(file: &mut Vec<u8>, block: &[u8], snappy: bool) -> Handle {
		let stored = if snappy {
			snap::raw::Encoder::new().compress_vec(block).unwrap()
		} else {
			block.to_vec()
		};
		let compression = u8::from(snappy);
		let mut hasher = crc32fast::Hasher::new();
		hasher.update(&stored);
		hasher.update(&[compression]);
		let handle = Handle {
			offset: file.len() as u64,
			size: stored.len() as u64,
		};
		file.extend_from_slice(&stored);
		file.push(compression);
		file.extend_from_slice(&mask(hasher.finalize()).to_le_bytes());
		handle
	}

	pub(crate) fn encode_handle(handle: Handle) -> Vec<u8> {
		let mut out = Vec::new();
		put_varint(&mut out, handle.offset);
		put_varint(&mut out, handle.size);
		out
	}

	/// An internal key as LSMV1 wrote it: `user_key | trailer | timestamp`.
	pub(crate) fn internal_key(user: &[u8], seq: u64, kind: u8, timestamp: u64) -> Vec<u8> {
		let mut key = user.to_vec();
		key.extend_from_slice(&((seq << 8) | u64::from(kind)).to_be_bytes());
		key.extend_from_slice(&timestamp.to_be_bytes());
		key
	}

	/// A table of the given format: its data blocks of `per_block` entries each, partitions of
	/// `per_partition` data blocks, the meta-index block with the properties under a key of
	/// `meta_suffix` bytes after `meta`, and the footer.
	pub(crate) fn table(
		entries: &[(Vec<u8>, Vec<u8>)],
		per_block: usize,
		per_partition: usize,
		snappy: bool,
		format: u8,
		meta_suffix: usize,
	) -> Vec<u8> {
		let mut file = Vec::new();
		let mut data_handles = Vec::new();
		for chunk in entries.chunks(per_block.max(1)) {
			let pairs: Vec<(&[u8], &[u8])> =
				chunk.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
			let handle = append(&mut file, &block(&pairs, 4), snappy);
			data_handles.push((chunk.last().unwrap().0.clone(), handle));
		}
		let mut partitions = Vec::new();
		for chunk in data_handles.chunks(per_partition.max(1)) {
			let encoded: Vec<(Vec<u8>, Vec<u8>)> =
				chunk.iter().map(|(k, h)| (k.clone(), encode_handle(*h))).collect();
			let pairs: Vec<(&[u8], &[u8])> =
				encoded.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
			let handle = append(&mut file, &block(&pairs, 4), snappy);
			partitions.push((chunk.last().unwrap().0.clone(), handle));
		}
		let encoded: Vec<(Vec<u8>, Vec<u8>)> =
			partitions.iter().map(|(k, h)| (k.clone(), encode_handle(*h))).collect();
		let pairs: Vec<(&[u8], &[u8])> =
			encoded.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
		let index = append(&mut file, &block(&pairs, 4), snappy);

		let mut meta_key = b"meta".to_vec();
		meta_key.extend(std::iter::repeat_n(0u8, meta_suffix));
		let meta_index = append(&mut file, &block(&[(&meta_key, b"properties")], 4), false);

		let mut footer = vec![0u8; 50];
		footer[0] = format;
		footer[1] = 1;
		let mut handles = encode_handle(meta_index);
		handles.extend(encode_handle(index));
		footer[2..2 + handles.len()].copy_from_slice(&handles);
		footer[42..].copy_from_slice(&[0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb]);
		file.extend_from_slice(&footer);
		file
	}
}
