//! Tests for RocksDB compatibility crate.

use crate::block::{decompress_block, Block};
use crate::handle::BlockHandle;
use crate::varint::decode_varint;

#[test]
fn test_varint_roundtrip() {
	let mut buf = Vec::new();
	let values = [0u64, 1, 127, 128, 255, 300, 16384, 1_000_000, u64::MAX];
	for &val in &values {
		let mut v = val;
		while v >= 0x80 {
			buf.push((v as u8 & 0x7f) | 0x80);
			v >>= 7;
		}
		buf.push(v as u8);
	}

	let mut offset = 0;
	for &expected in &values {
		let (val, len) = decode_varint(&buf, offset).unwrap();
		assert_eq!(val, expected);
		offset += len;
	}
	assert_eq!(offset, buf.len());
}

#[test]
fn test_block_handle_decode() {
	// 1024 -> 0x80, 0x08; 4096 -> 0x80, 0x20
	let buf = vec![0x80, 0x08, 0x80, 0x20];

	let (handle, len) = BlockHandle::decode(&buf, 0).unwrap();
	assert_eq!(handle.offset, 1024);
	assert_eq!(handle.size, 4096);
	assert_eq!(len, 4);
}

#[test]
fn test_decompression_uncompressed() {
	let data = b"hello rocksdb block data";
	let decompressed = decompress_block(data, 0).unwrap();
	assert_eq!(decompressed, data);
}

#[test]
fn test_decompression_snappy() {
	let original = b"repeated text for compression testing repeated text repeated text";
	let mut encoder = snap::raw::Encoder::new();
	let compressed = encoder.compress_vec(original).unwrap();

	let decompressed = decompress_block(&compressed, 1).unwrap();
	assert_eq!(decompressed, original);
}

#[test]
fn test_decompression_zstd() {
	let original = b"zstandard compressed rocksdb block with high ratio testing";
	let compressed = zstd::encode_all(&original[..], 3).unwrap();

	let decompressed = decompress_block(&compressed, 7).unwrap();
	assert_eq!(decompressed, original);
}

#[test]
fn test_decompression_lz4() {
	let original = b"lz4 compressed rocksdb block data for fast streaming reads";
	let compressed = lz4_flex::compress_prepend_size(original);

	let decompressed = decompress_block(&compressed, 4).unwrap();
	assert_eq!(decompressed, original);
}

#[test]
fn test_block_parsing_with_restarts() {
	// Construct a Block with 2 entries:
	// Entry 1: shared=0, unshared=4, val_len=5, key="user", val="alice"
	// Entry 2: shared=4, unshared=2, val_len=3, key="user:2", val="bob"
	// Restarts: [0], num_restarts = 1
	let mut data = Vec::new();
	// Entry 1
	data.extend_from_slice(&[0, 4, 5]);
	data.extend_from_slice(b"user");
	data.extend_from_slice(b"alice");

	// Entry 2
	data.extend_from_slice(&[4, 2, 3]);
	data.extend_from_slice(b":2");
	data.extend_from_slice(b"bob");

	// Restarts
	data.extend_from_slice(&0u32.to_le_bytes()); // restart offset 0
	data.extend_from_slice(&1u32.to_le_bytes()); // num_restarts = 1

	let block = Block::new(data).unwrap();
	let entries: Vec<_> = block.iter().map(|r| r.unwrap()).collect();

	assert_eq!(entries.len(), 2);
	assert_eq!(entries[0].0, b"user");
	assert_eq!(entries[0].1, b"alice");
	assert_eq!(entries[1].0, b"user:2");
	assert_eq!(entries[1].1, b"bob");
}
