//! The layout of a sealed (encrypted) WAL segment, and what of it can be made and checked without
//! a file.
//!
//! A sealed segment is framed as a plaintext one is, in blocks of physical records with a checksum
//! each, and differs in two things only.
//!
//! Its first record is a `SegmentHeader`, which holds the encryption header of the segment and what
//! the segment says about itself:
//!
//! ```text
//!  0  92  the encryption header, for a WAL file whose id is the number of the segment
//! 92   1  the format of the rest of the segment, 1
//! 93   1  how the records are compressed before they are sealed: 0 none, 1 LZ4
//! ```
//!
//! The tag of the encryption header covers the last two bytes, so neither can change without the
//! header failing to open. There is no `SetCompressionType` record in a sealed segment: it carries
//! nothing that is authenticated, so a reader refuses it.
//!
//! Every other record is one batch, sealed as a single AEAD message whose stored form is the
//! ciphertext followed by the tag, and framed as the batch would be if it were plaintext, in as
//! many physical records as its length needs. A batch is sealed at the position equal to its
//! index among the batches of its segment, counted from 1, so the index is not stored: the reader
//! counts, and a batch that was moved, repeated or taken out does not open at the index it finds
//! it at. The checksum of each physical record is of the sealed bytes and stays outside of the
//! message: it is how a write that was cut short is told from a record that was changed, which has
//! a good checksum and a tag that fails.
//!
//! The batch is compressed before it is sealed, as a size and an LZ4 block, if the header says so.

use crate::error::EncryptionError;
use crate::tde::{
	BlockContext,
	BlockKind,
	CryptoHeader,
	EncryptionContext,
	FileCipher,
	FileKind,
	HEADER_LEN,
};
use crate::wal::{CompressionType, Error, IOError, Result};

/// The format of a sealed segment that this build writes and reads.
pub(crate) const FORMAT_VERSION: u8 = 1;

/// What the tag of the encryption header covers besides the header: the format and the
/// compression of the segment.
const COVERED_LEN: usize = 2;

/// The length of the payload of the `SegmentHeader` record.
pub(crate) const HEADER_PAYLOAD_LEN: usize = HEADER_LEN + COVERED_LEN;

/// The length of the header record of a sealed segment, framing included: where its first batch
/// begins.
pub(crate) const HEADER_RECORD_LEN: usize = super::HEADER_SIZE + HEADER_PAYLOAD_LEN;

/// The index of the first batch of a segment. Position 0 of a file key is not used by a WAL.
pub(crate) const FIRST_INDEX: u64 = 1;

/// An LZ4 block cannot expand to more than this many bytes for each byte it takes: the longest
/// run of a match is described by one byte per 255 bytes of output.
const LZ4_MAX_RATIO: usize = 255;

/// The bytes the tag of the encryption header covers, for a segment compressed as given.
fn covered_bytes(compression: CompressionType) -> [u8; COVERED_LEN] {
	[FORMAT_VERSION, compression as u8]
}

/// The payload of the `SegmentHeader` record of a new segment `segment_id`, and the cipher that
/// seals its batches.
///
/// Every call draws a new salt and so a new key, whatever the segment id: a segment that is made
/// again, after a failure or under a number that a segment of an earlier run had, never shares a
/// key with it.
pub(crate) fn new_header(
	context: &EncryptionContext,
	segment_id: u64,
	compression: CompressionType,
) -> std::result::Result<([u8; HEADER_PAYLOAD_LEN], FileCipher), EncryptionError> {
	let covered = covered_bytes(compression);
	let (header, cipher) = context.create_file(FileKind::Wal, segment_id, &covered)?;
	let mut payload = [0u8; HEADER_PAYLOAD_LEN];
	payload[..HEADER_LEN].copy_from_slice(&header.encode());
	payload[HEADER_LEN..].copy_from_slice(&covered);
	Ok((payload, cipher))
}

/// Checks the payload of the `SegmentHeader` record of the segment `segment_id`, which passed the
/// checksum of its record, and returns the cipher that opens its batches and how they are
/// compressed.
///
/// In order: the encryption header is well formed (`MalformedHeader`, `UnsupportedVersion`,
/// `UnsupportedSuite`), it is for this database and this segment (`DbIdMismatch`,
/// `FileIdMismatch`), its key can be had (`UnknownKeyId`, `KeyRetired`, what the manager fails
/// with) and its tag holds (`WrongKey`). Only then are the two bytes after it read, so none of them
/// is believed before it authenticated, and one that this build does not know is
/// `UnsupportedVersion`. No failure is damage, and none is a reason to touch the segment.
///
/// `prepare` is asked for the key the header names, which may wait on a key provider: recovery
/// runs at open, where that is allowed, and a read of a block never would.
pub(crate) fn open_header(
	context: &EncryptionContext,
	segment_id: u64,
	payload: &[u8],
) -> std::result::Result<(FileCipher, CompressionType), EncryptionError> {
	let header = CryptoHeader::decode(payload)?;
	let covered = payload.get(HEADER_LEN..).ok_or(EncryptionError::MalformedHeader)?;
	context.prepare(&[header.identity.key_id])?;
	let cipher = context.open_file(&header, FileKind::Wal, segment_id, covered)?;

	match covered {
		[FORMAT_VERSION, compression] => {
			let compression = CompressionType::from_u8(*compression)
				.map_err(|_| EncryptionError::UnsupportedVersion)?;
			Ok((cipher, compression))
		}
		_ => Err(EncryptionError::UnsupportedVersion),
	}
}

/// What the batch with `index` in its segment is bound to, besides its file: where it is, what it
/// holds, and how it was compressed.
pub(crate) fn record_context(index: u64, compression: CompressionType) -> BlockContext {
	BlockContext::new(BlockKind::WalBatch, index, compression as u8)
}

/// Decompresses the plaintext of a batch that was compressed with LZ4, which opened: a size, then
/// the block.
///
/// The bytes are authentic, so the size is what a writer of the segment wrote, but a bug of a
/// writer must not be an allocation of the size of a `u32` either: the size is held to what a block
/// of this length can hold.
pub(crate) fn decompress(plain: &[u8]) -> Result<Vec<u8>> {
	let bad = |what: &str| Error::IO(IOError::new(std::io::ErrorKind::InvalidData, what));
	let (size, block) = plain.split_first_chunk::<4>().ok_or_else(|| bad("a short LZ4 record"))?;
	let size = usize::try_from(u32::from_le_bytes(*size))
		.map_err(|_| bad("an LZ4 record that is too long"))?;
	if size > block.len().saturating_mul(LZ4_MAX_RATIO) {
		return Err(bad("an LZ4 record claims more than its block can hold"));
	}
	lz4_flex::block::decompress(block, size)
		.map_err(|e| bad(&format!("LZ4 decompression failed: {e}")))
}
