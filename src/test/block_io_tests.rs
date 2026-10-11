//! Tests of the trailer layouts, of `BlockSink` and `BlockSource`, and of what they do with a
//! sealer.
//!
//! There is no real sealer yet. `ToySealer` is one that is good enough to tell the tests whether
//! the seam gives a sealer what it must be given, and whether a block that does not open is
//! stopped before it is decompressed. It is not a cipher.

use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{EncryptionError, Error, KeyProviderFailure, Result};
use crate::sstable::block::BlockHandle;
use crate::sstable::block_io::{
	verify_trailer,
	BlockContext,
	BlockKind,
	BlockSealer,
	BlockSink,
	BlockSource,
	FileStart,
	FileStarter,
	Protection,
	Trailer,
	TrailerLayout,
	PLAIN_TRAILER_LEN,
	SEALED_TRAILER_LEN,
};
use crate::sstable::error::SSTableError;
use crate::sstable::table::{
	mask,
	ChecksumType,
	Footer,
	Table,
	TableWriter,
	TABLE_FULL_FOOTER_LENGTH,
};
use crate::vfs::File;
use crate::{CompressionType, InternalKey, InternalKeyKind, Options, INTERNAL_KEY_SEQ_NUM_MAX};

const ALL_KINDS: [BlockKind; 6] = [
	BlockKind::Data,
	BlockKind::IndexPartition,
	BlockKind::TopLevelIndex,
	BlockKind::FilterPartition,
	BlockKind::FilterIndex,
	BlockKind::MetaIndex,
];

fn hex(digits: &str) -> Vec<u8> {
	(0..digits.len() / 2)
		.map(|i| u8::from_str_radix(&digits[2 * i..2 * i + 2], 16).unwrap())
		.collect()
}

// ========== Sealers ==========

/// A sealer that does nothing and adds nothing: the sealed layout with no cipher in it.
struct NoopSealer;

impl BlockSealer for NoopSealer {
	fn overhead(&self) -> usize {
		0
	}

	fn seal(&self, _: &BlockContext, _: &mut Vec<u8>) -> Result<()> {
		Ok(())
	}

	fn open(&self, _: &BlockContext, _: &mut Vec<u8>) -> Result<()> {
		Ok(())
	}
}

const TOY_TAG_LEN: usize = 16;

/// Scrambles a block with a keystream and adds a 16 byte tag that depends on the sealed bytes,
/// the kind of the block, its position and its compression byte. A block opens only in the place
/// it was sealed for. It keeps a ledger of what it was asked to seal and to open.
struct ToySealer {
	key: u8,
	sealed: Mutex<Vec<(BlockKind, u64, usize)>>,
	opened: Mutex<Vec<(BlockKind, u64)>>,
}

impl ToySealer {
	fn new(key: u8) -> Arc<Self> {
		Arc::new(Self {
			key,
			sealed: Mutex::new(Vec::new()),
			opened: Mutex::new(Vec::new()),
		})
	}

	fn stream(&self, block: &mut [u8], context: &BlockContext) {
		for (i, byte) in block.iter_mut().enumerate() {
			*byte ^= self.key.wrapping_add(i as u8).wrapping_add(context.kind as u8);
		}
	}

	fn tag(&self, context: &BlockContext, sealed: &[u8]) -> [u8; TOY_TAG_LEN] {
		let mut tag = [0u8; TOY_TAG_LEN];
		for round in 0..4u8 {
			let mut crc = crc32fast::Hasher::new();
			crc.update(&[self.key, round, context.kind as u8, context.compression]);
			crc.update(&context.offset.to_le_bytes());
			crc.update(sealed);
			let at = usize::from(round) * 4;
			tag[at..at + 4].copy_from_slice(&crc.finalize().to_le_bytes());
		}
		tag
	}

	fn sealed(&self) -> Vec<(BlockKind, u64, usize)> {
		self.sealed.lock().unwrap().clone()
	}

	fn opened(&self) -> Vec<(BlockKind, u64)> {
		self.opened.lock().unwrap().clone()
	}
}

impl BlockSealer for ToySealer {
	fn overhead(&self) -> usize {
		TOY_TAG_LEN
	}

	fn seal(&self, context: &BlockContext, block: &mut Vec<u8>) -> Result<()> {
		self.stream(block, context);
		let tag = self.tag(context, block);
		block.extend_from_slice(&tag);
		self.sealed.lock().unwrap().push((context.kind, context.offset, block.len()));
		Ok(())
	}

	fn open(&self, context: &BlockContext, block: &mut Vec<u8>) -> Result<()> {
		self.opened.lock().unwrap().push((context.kind, context.offset));
		let Some(at) = block.len().checked_sub(TOY_TAG_LEN) else {
			return Err(Error::Corruption("toy sealer: block shorter than its tag".into()));
		};
		let tag = block.split_off(at);
		if tag != self.tag(context, block) {
			block.clear();
			return Err(Error::Corruption("toy sealer: authentication failed".into()));
		}
		self.stream(block, context);
		Ok(())
	}
}

/// The protection of a file whose blocks `sealer` seals.
fn sealed_by(sealer: &Arc<ToySealer>) -> Protection {
	let shared: Arc<ToySealer> = Arc::clone(sealer);
	Protection::Sealed(shared)
}

fn is_toy_failure(err: &Error) -> bool {
	matches!(err, Error::Corruption(message) if message.starts_with("toy sealer"))
}

// ========== Trailers ==========

/// A payload, a compression byte, and the trailers that go after it, from `zlib.crc32` and the
/// mask of the format, worked out outside of this code base.
fn trailer_vectors() -> Vec<(Vec<u8>, u8, &'static str, &'static str)> {
	vec![
		(b"hello".to_vec(), 0, "00b7980055", "0005000000984f0510"),
		(b"hello".to_vec(), 1, "01a92ed475", "0105000000d80865e1"),
		(Vec::new(), 0, "00dd8e9e81", "00000000001d77be90"),
		((0..40u8).collect(), 2, "02b411934a", "0228000000df14f685"),
	]
}

#[test]
fn the_two_layouts_have_trailers_of_five_and_nine_bytes() {
	assert_eq!(TrailerLayout::Plain.len(), 5);
	assert_eq!(TrailerLayout::Sealed.len(), 9);
	assert_eq!(PLAIN_TRAILER_LEN, 5);
	assert_eq!(SEALED_TRAILER_LEN, 9);
	assert_eq!(Protection::Plain.layout(), TrailerLayout::Plain);
	assert_eq!(Protection::Sealed(Arc::new(NoopSealer)).layout(), TrailerLayout::Sealed);
}

#[test]
fn the_numbers_of_the_block_kinds_are_fixed() {
	// A sealer binds them to the block, so they are not to change.
	assert_eq!(ALL_KINDS.map(|kind| kind as u8), [1, 2, 3, 4, 5, 6]);
}

#[test]
fn a_plain_trailer_is_the_compression_byte_and_the_masked_crc() {
	for (payload, compression, plain, _) in trailer_vectors() {
		let trailer = Trailer::encode(TrailerLayout::Plain, &payload, compression).unwrap();
		assert_eq!(trailer.as_slice(), hex(plain), "payload of {} bytes", payload.len());
	}
}

#[test]
fn a_sealed_trailer_adds_the_length_between_the_compression_byte_and_the_crc() {
	for (payload, compression, _, sealed) in trailer_vectors() {
		let trailer = Trailer::encode(TrailerLayout::Sealed, &payload, compression).unwrap();
		assert_eq!(trailer.as_slice(), hex(sealed), "payload of {} bytes", payload.len());
	}
}

/// A unit that is as the sealed layout makes it, with the length field that the caller says.
fn forged_sealed_unit(payload: &[u8], compression: u8, len: u32) -> Vec<u8> {
	let mut unit = payload.to_vec();
	unit.push(compression);
	unit.extend_from_slice(&len.to_le_bytes());
	let crc = crc32fast::hash(&unit);
	unit.extend_from_slice(&mask(crc).to_le_bytes());
	unit
}

#[test]
fn a_trailer_that_was_encoded_verifies_and_gives_the_compression_byte() {
	for layout in [TrailerLayout::Plain, TrailerLayout::Sealed] {
		for (payload, compression, _, _) in trailer_vectors() {
			let mut unit = payload.clone();
			unit.extend_from_slice(
				Trailer::encode(layout, &payload, compression).unwrap().as_slice(),
			);
			assert_eq!(verify_trailer(layout, &unit, 0).unwrap(), compression);
		}
	}
	// The helper that forges units agrees with the vectors, so that the tests that use it
	// to forge a length are forging the real thing.
	let (payload, compression, _, sealed) = trailer_vectors().remove(0);
	let mut expected = payload.clone();
	expected.extend_from_slice(&hex(sealed));
	assert_eq!(forged_sealed_unit(&payload, compression, payload.len() as u32), expected);
}

#[test]
fn any_single_bit_flip_in_a_block_or_its_trailer_fails_the_checksum() {
	for layout in [TrailerLayout::Plain, TrailerLayout::Sealed] {
		for (payload, compression, _, _) in trailer_vectors() {
			let mut unit = payload.clone();
			unit.extend_from_slice(
				Trailer::encode(layout, &payload, compression).unwrap().as_slice(),
			);
			for bit in 0..unit.len() * 8 {
				let mut flipped = unit.clone();
				flipped[bit / 8] ^= 1 << (bit % 8);
				let result = verify_trailer(layout, &flipped, 42);
				assert!(
					matches!(
						result,
						Err(Error::SSTable(SSTableError::ChecksumVerificationFailed {
							block_offset: 42
						}))
					),
					"{layout:?}, payload of {} bytes, bit {bit}: {result:?}",
					payload.len()
				);
			}
		}
	}
}

#[test]
fn a_length_that_is_not_the_length_of_the_block_is_corruption_even_with_a_valid_checksum() {
	for wrong in [0u32, 4, 6, 5 + 9, u32::MAX] {
		let unit = forged_sealed_unit(b"hello", 0, wrong);
		let result = verify_trailer(TrailerLayout::Sealed, &unit, 7);
		assert!(
			matches!(
				result,
				Err(Error::SSTable(SSTableError::TrailerLengthMismatch {
					block_offset: 7,
					block_size: 5,
					trailer_len,
				})) if trailer_len == u64::from(wrong)
			),
			"length {wrong}: {result:?}"
		);
	}
	// And the right one is accepted by the same code.
	assert_eq!(
		verify_trailer(TrailerLayout::Sealed, &forged_sealed_unit(b"hello", 0, 5), 7).unwrap(),
		0
	);
}

#[test]
fn a_unit_shorter_than_its_trailer_is_an_error() {
	for (layout, len) in [(TrailerLayout::Plain, 5), (TrailerLayout::Sealed, 9)] {
		for short in 0..len {
			let result = verify_trailer(layout, &vec![0u8; short], 0);
			assert!(
				matches!(result, Err(Error::SSTable(SSTableError::CorruptedBlockHandle))),
				"{layout:?}, {short} bytes: {result:?}"
			);
		}
	}
}

// ========== The bytes that a sink writes ==========

/// What the footer holds for a table whose meta-index is the handle `(meta, 0)` and whose
/// top-level index is the handle `(0, 5)`: format 1, the checksum type that says how the blocks
/// are laid out, the handles as varints, zeros up to byte 42 and the magic number.
fn expected_footer(checksum_type: u8, meta_offset: u8) -> String {
	expected_footer_of(checksum_type, (meta_offset, 0), (0, 5))
}

/// The footer of a table with the handles `meta` and `index`, which are small enough to be one
/// byte each in a varint.
fn expected_footer_of(checksum_type: u8, meta: (u8, u8), index: (u8, u8)) -> String {
	format!(
		"01{checksum_type:02x}{:02x}{:02x}{:02x}{:02x}{}57fb808b247547db",
		meta.0,
		meta.1,
		index.0,
		index.1,
		"00".repeat(36)
	)
}

#[test]
fn a_sink_writes_blocks_with_the_plain_trailer_and_a_footer() {
	let mut file = Vec::new();
	let mut sink = BlockSink::new(&mut file);
	let first =
		sink.write_block(BlockKind::Data, b"hello".to_vec(), CompressionType::None).unwrap();
	assert_eq!(sink.offset(), 10);
	let second = sink.write_block(BlockKind::MetaIndex, Vec::new(), CompressionType::None).unwrap();
	let size = sink.write_footer(&Footer::new(second.clone(), first.clone())).unwrap();

	assert_eq!((first, second), (BlockHandle::new(0, 5), BlockHandle::new(10, 0)));
	assert_eq!(size, 65);
	let expected =
		[hex("68656c6c6f"), hex("00b7980055"), hex("00dd8e9e81"), hex(&expected_footer(1, 10))]
			.concat();
	assert_eq!(file, expected);
}

#[test]
fn a_sink_with_a_sealer_writes_blocks_with_the_sealed_trailer_and_a_footer() {
	let mut file = Vec::new();
	let mut sink = BlockSink::with_protection(&mut file, Protection::Sealed(Arc::new(NoopSealer)));
	let first =
		sink.write_block(BlockKind::Data, b"hello".to_vec(), CompressionType::None).unwrap();
	assert_eq!(sink.offset(), 14);
	let second = sink.write_block(BlockKind::MetaIndex, Vec::new(), CompressionType::None).unwrap();
	let size = sink.write_footer(&Footer::new(second.clone(), first.clone())).unwrap();

	assert_eq!((first.clone(), second.clone()), (BlockHandle::new(0, 5), BlockHandle::new(14, 0)));
	assert_eq!(size, 73);
	let expected = [
		hex("68656c6c6f"),
		hex("0005000000984f0510"),
		hex("00000000001d77be90"),
		hex(&expected_footer(2, 14)),
	]
	.concat();
	assert_eq!(file, expected);

	// And a source reads the same bytes back, which is the layout without a cipher.
	let source = BlockSource::with_protection(
		Arc::new(file),
		size as u64,
		Protection::Sealed(Arc::new(NoopSealer)),
	);
	assert_eq!(source.read_contents(BlockKind::Data, &first).unwrap(), b"hello");
	assert_eq!(source.read_contents(BlockKind::MetaIndex, &second).unwrap(), Vec::<u8>::new());
}

/// Fails the `fail_on`th call of `write`, and takes all of what the others are given.
struct FailingWriter {
	written: Vec<u8>,
	calls: usize,
	fail_on: usize,
}

impl Write for FailingWriter {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		self.calls += 1;
		if self.calls == self.fail_on {
			return Err(io::Error::other("the disk is full"));
		}
		self.written.extend_from_slice(buf);
		Ok(buf.len())
	}

	fn flush(&mut self) -> io::Result<()> {
		Ok(())
	}
}

#[test]
fn a_sink_whose_write_failed_writes_nothing_more() {
	let mut writer = FailingWriter {
		written: Vec::new(),
		calls: 0,
		fail_on: 2,
	};
	{
		let mut sink = BlockSink::new(&mut writer);
		// The block is written, and the trailer fails.
		assert!(sink
			.write_block(BlockKind::Data, b"hello".to_vec(), CompressionType::None)
			.is_err());
		// The position was not taken, and nothing can go after bytes that are not known.
		assert_eq!(sink.offset(), 0);
		assert!(sink
			.write_block(BlockKind::Data, b"world".to_vec(), CompressionType::None)
			.is_err());
		let footer = Footer::new(BlockHandle::new(0, 5), BlockHandle::new(0, 5));
		assert!(sink.write_footer(&footer).is_err());
	}
	assert_eq!(writer.calls, 2, "no write was attempted after the one that failed");
	assert_eq!(writer.written, b"hello");
}

/// Seals nothing and adds nothing, and fails its `fail_on`th call of `seal`. It counts the calls.
struct FailingSealer {
	calls: AtomicUsize,
	fail_on: usize,
}

impl BlockSealer for FailingSealer {
	fn overhead(&self) -> usize {
		0
	}

	fn seal(&self, _: &BlockContext, _: &mut Vec<u8>) -> Result<()> {
		let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
		if call == self.fail_on {
			return Err(Error::Other("the key could not be had".into()));
		}
		Ok(())
	}

	fn open(&self, _: &BlockContext, _: &mut Vec<u8>) -> Result<()> {
		Ok(())
	}
}

#[test]
fn a_sink_whose_sealer_failed_does_not_ask_it_for_that_position_again() {
	let sealer = Arc::new(FailingSealer {
		calls: AtomicUsize::new(0),
		fail_on: 2,
	});
	let shared: Arc<FailingSealer> = Arc::clone(&sealer);
	let mut file = Vec::new();
	{
		let mut sink = BlockSink::with_protection(&mut file, Protection::Sealed(shared));
		sink.write_block(BlockKind::Data, b"hello".to_vec(), CompressionType::None).unwrap();
		assert_eq!(sink.offset(), 14);

		// The second block fails in the sealer, at the position 14.
		let err = sink
			.write_block(BlockKind::Data, b"world".to_vec(), CompressionType::None)
			.unwrap_err();
		assert!(matches!(err, Error::Other(_)), "{err:?}");
		assert_eq!(sink.offset(), 14, "the position was not taken");

		// Another block would be sealed at the same position: it is refused, and the sealer is
		// not asked, though it would now succeed.
		assert!(sink
			.write_block(BlockKind::MetaIndex, b"again".to_vec(), CompressionType::None)
			.is_err());
		let footer = Footer::new(BlockHandle::new(0, 5), BlockHandle::new(0, 5));
		assert!(sink.write_footer(&footer).is_err());
	}
	assert_eq!(sealer.calls.load(Ordering::SeqCst), 2);
	assert_eq!(file.len(), 14, "nothing was written after the block that failed");
}

// ========== A sealed table ==========

fn table_options(compression: CompressionType) -> Arc<Options> {
	Arc::new(Options {
		block_size: 256,
		block_restart_interval: 4,
		index_partition_size: 100,
		compression_per_level: vec![compression],
		..Default::default()
	})
}

const KEYS: usize = 120;

fn value(i: usize) -> Vec<u8> {
	format!("value{i:04}-{}", "ab".repeat(20)).into_bytes()
}

fn lookup(i: usize) -> InternalKey {
	InternalKey::new(
		format!("key{i:04}").into_bytes(),
		INTERNAL_KEY_SEQ_NUM_MAX,
		InternalKeyKind::Set,
	)
}

fn write_table(opts: &Arc<Options>, protection: Protection) -> Vec<u8> {
	let mut file = Vec::new();
	{
		let sink = BlockSink::with_protection(&mut file, protection);
		let mut writer = TableWriter::with_sink(sink, 1, Arc::clone(opts), 0);
		for i in 0..KEYS {
			let key = InternalKey::new(
				format!("key{i:04}").into_bytes(),
				(i + 1) as u64,
				InternalKeyKind::Set,
			);
			writer.add(key, &value(i)).unwrap();
		}
		writer.finish().unwrap();
	}
	file
}

fn open_table(opts: &Arc<Options>, file: Vec<u8>, protection: Protection) -> Result<Table> {
	let size = file.len() as u64;
	Table::open_with(1, Arc::clone(opts), Arc::new(file), size, |_| Ok(protection))
}

#[test]
fn a_table_is_written_and_read_with_every_block_sealed() {
	for compression in [CompressionType::None, CompressionType::SnappyCompression] {
		let opts = table_options(compression);
		let sealer = ToySealer::new(0x5a);
		let file = write_table(&opts, sealed_by(&sealer));

		// All six kinds of block went through the sealer, each at the place where it is.
		let sealed = sealer.sealed();
		let mut kinds: Vec<u8> = sealed.iter().map(|(kind, ..)| *kind as u8).collect();
		kinds.sort_unstable();
		kinds.dedup();
		assert_eq!(kinds, ALL_KINDS.map(|kind| kind as u8), "{compression:?}");

		// They are one after the other from the start of the file to the footer, each with a
		// trailer of nine bytes that says how long it is, and that checks.
		let mut ordered = sealed.clone();
		ordered.sort_by_key(|(_, offset, _)| *offset);
		let mut expected = 0u64;
		for (kind, offset, len) in &ordered {
			assert_eq!(*offset, expected, "{compression:?}: the {kind:?} block at {offset}");
			let end = *offset as usize + len + SEALED_TRAILER_LEN;
			let unit = &file[*offset as usize..end];
			verify_trailer(TrailerLayout::Sealed, unit, *offset).unwrap();
			expected = end as u64;
		}
		assert_eq!(expected as usize, file.len() - TABLE_FULL_FOOTER_LENGTH, "{compression:?}");

		// Every key is read back, which reads every kind of block from where it was written.
		let table = open_table(&opts, file, sealed_by(&sealer)).unwrap();
		for i in 0..KEYS {
			let (key, found) = table.get(&lookup(i)).unwrap().expect("the key is in the table");
			assert_eq!(key.user_key, format!("key{i:04}").into_bytes(), "{compression:?}");
			assert_eq!(found, value(i), "{compression:?}");
		}

		// And each block was opened for the kind and at the offset that it was sealed for.
		let opened = sealer.opened();
		let mut kinds: Vec<u8> = opened.iter().map(|(kind, _)| *kind as u8).collect();
		kinds.sort_unstable();
		kinds.dedup();
		assert_eq!(kinds, ALL_KINDS.map(|kind| kind as u8), "{compression:?}");
		for (kind, offset) in opened {
			assert!(
				sealed.iter().any(|(k, o, _)| (*k, *o) == (kind, offset)),
				"{compression:?}: a {kind:?} block was opened at {offset}, where none was sealed"
			);
		}
	}
}

#[tokio::test]
async fn the_asynchronous_path_opens_sealed_blocks_too() {
	let opts = table_options(CompressionType::SnappyCompression);
	let sealer = ToySealer::new(0x21);
	let file = write_table(&opts, sealed_by(&sealer));
	let table = open_table(&opts, file, sealed_by(&sealer)).unwrap();

	for i in (0..KEYS).step_by(7) {
		let (_, found) = table.get_async(&lookup(i)).await.unwrap().expect("the key is there");
		assert_eq!(found, value(i));
	}
	assert!(sealer.opened().iter().any(|(kind, _)| *kind == BlockKind::Data));
}

#[test]
fn a_table_read_with_the_wrong_layout_is_an_error_and_is_never_opened() {
	let opts = table_options(CompressionType::None);

	// A table with sealed blocks, read as if its blocks were plain: the checksum is not where
	// it is looked for.
	let sealer = ToySealer::new(1);
	let sealed = write_table(&opts, sealed_by(&sealer));
	let result = open_table(&opts, sealed, Protection::Plain);
	assert!(
		matches!(result, Err(Error::SSTable(SSTableError::ChecksumVerificationFailed { .. }))),
		"a sealed table was opened as a plain one"
	);

	// A plain table, read as if its blocks were sealed: nothing reaches the sealer.
	let plain = write_table(&opts, Protection::Plain);
	let result = open_table(&opts, plain, sealed_by(&sealer));
	assert!(result.is_err(), "a plain table was opened as a sealed one");
	assert!(sealer.opened().is_empty(), "a block that failed its checksum reached the sealer");
}

// ========== Sealed blocks, one by one ==========

/// Three blocks with the compression that each of the three types gives them, sealed by `sealer`
/// into a file with a footer, with the handles of the blocks and a source that reads them.
fn sealed_file(sealer: &Arc<ToySealer>) -> (Vec<u8>, [BlockHandle; 3], BlockSource) {
	let blocks = [
		(BlockKind::Data, CompressionType::SnappyCompression, "abcdefgh".repeat(40)),
		(BlockKind::IndexPartition, CompressionType::ZstdCompression, "index".repeat(8)),
		(BlockKind::MetaIndex, CompressionType::None, "meta".to_string()),
	];
	let mut file = Vec::new();
	let mut sink = BlockSink::with_protection(&mut file, sealed_by(sealer));
	let handles = blocks.map(|(kind, compression, contents)| {
		sink.write_block(kind, contents.into_bytes(), compression).unwrap()
	});
	sink.write_footer(&Footer::new(handles[2].clone(), handles[1].clone())).unwrap();
	let size = file.len() as u64;
	let source = BlockSource::with_protection(Arc::new(file.clone()), size, sealed_by(sealer));
	(file, handles, source)
}

#[test]
fn sealed_blocks_come_back_decompressed() {
	let sealer = ToySealer::new(9);
	let (_, handles, source) = sealed_file(&sealer);

	assert_eq!(
		source.read_contents(BlockKind::Data, &handles[0]).unwrap(),
		"abcdefgh".repeat(40).into_bytes()
	);
	assert_eq!(
		source.read_contents(BlockKind::IndexPartition, &handles[1]).unwrap(),
		"index".repeat(8).into_bytes()
	);
	assert_eq!(source.read_contents(BlockKind::MetaIndex, &handles[2]).unwrap(), b"meta");
	// The compressed ones are smaller than what was put in: they were compressed, and then
	// sealed, which is the order that opening undoes.
	assert!(handles[0].size < 8 * 40 + TOY_TAG_LEN);
}

#[test]
fn a_block_does_not_open_as_a_block_of_another_kind() {
	let sealer = ToySealer::new(9);
	let (_, handles, source) = sealed_file(&sealer);

	let err = source.read_contents(BlockKind::IndexPartition, &handles[0]).unwrap_err();
	assert!(is_toy_failure(&err), "{err:?}");
	let err = source.read_contents(BlockKind::Data, &handles[2]).unwrap_err();
	assert!(is_toy_failure(&err), "{err:?}");
}

#[test]
fn a_block_does_not_open_at_a_place_other_than_the_one_it_was_sealed_for() {
	let sealer = ToySealer::new(9);
	let (file, handles, _) = sealed_file(&sealer);

	// The same bytes, 64 bytes further into the file: the trailers still check, and they are the
	// only thing that does.
	let mut moved = vec![0u8; 64];
	moved.extend_from_slice(&file);
	let size = moved.len() as u64;
	let source = BlockSource::with_protection(Arc::new(moved), size, sealed_by(&sealer));
	let shifted = BlockHandle::new(handles[0].offset + 64, handles[0].size);
	let err = source.read_contents(BlockKind::Data, &shifted).unwrap_err();
	assert!(is_toy_failure(&err), "{err:?}");
}

#[test]
fn a_block_that_does_not_open_is_not_decompressed() {
	let sealer = ToySealer::new(9);
	let (file, handles, source) = sealed_file(&sealer);
	let handle = &handles[0];
	assert!(source.read_contents(BlockKind::Data, handle).is_ok());

	// Damage a byte of the sealed block, and make the trailer say that it is as it should be, as
	// an attacker who does not know the key can. The bytes are Snappy's, and are not valid Snappy
	// when scrambled: reaching the decompressor would be another error.
	let mut file = file;
	file[handle.offset + 3] ^= 0x40;
	let payload = file[handle.offset..handle.offset + handle.size].to_vec();
	let compression = file[handle.offset + handle.size];
	let trailer = Trailer::encode(TrailerLayout::Sealed, &payload, compression).unwrap();
	let at = handle.offset + handle.size;
	file[at..at + SEALED_TRAILER_LEN].copy_from_slice(trailer.as_slice());

	let size = file.len() as u64;
	let source = BlockSource::with_protection(Arc::new(file), size, sealed_by(&sealer));
	let err = source.read_contents(BlockKind::Data, handle).unwrap_err();
	assert!(is_toy_failure(&err), "{err:?}");
	let err = match source.read_block(
		BlockKind::Data,
		handle,
		Arc::clone(&Options::default().internal_comparator),
	) {
		Ok(_) => panic!("a damaged block was decoded"),
		Err(err) => err,
	};
	assert!(is_toy_failure(&err), "{err:?}");
}

#[test]
fn a_sealed_block_shorter_than_the_seal_is_refused_without_being_opened() {
	let sealer = ToySealer::new(9);
	// Three bytes with a trailer that is right in every way.
	let mut file = forged_sealed_unit(b"abc", 0, 3);
	file.extend_from_slice(&[0u8; TABLE_FULL_FOOTER_LENGTH]);
	let size = file.len() as u64;
	let source = BlockSource::with_protection(Arc::new(file), size, sealed_by(&sealer));

	let result = source.read_contents(BlockKind::Data, &BlockHandle::new(0, 3));
	assert!(
		matches!(
			result,
			Err(Error::SSTable(SSTableError::SealedBlockTooShort {
				block_offset: 0,
				size: 3,
				min_size: 16
			}))
		),
		"{result:?}"
	);
	assert!(sealer.opened().is_empty());
}

#[test]
fn a_sealed_block_with_a_length_that_disagrees_with_its_handle_is_refused_without_being_opened() {
	let sealer = ToySealer::new(9);
	let mut file = forged_sealed_unit(&[7u8; 20], 0, 21);
	file.extend_from_slice(&[0u8; TABLE_FULL_FOOTER_LENGTH]);
	let size = file.len() as u64;
	let source = BlockSource::with_protection(Arc::new(file), size, sealed_by(&sealer));

	let result = source.read_contents(BlockKind::Data, &BlockHandle::new(0, 20));
	assert!(
		matches!(
			result,
			Err(Error::SSTable(SSTableError::TrailerLengthMismatch {
				block_size: 20,
				trailer_len: 21,
				..
			}))
		),
		"{result:?}"
	);
	assert!(sealer.opened().is_empty());
}

#[test]
fn a_table_with_sealed_blocks_has_no_legacy_filter() {
	let sealer = ToySealer::new(9);
	let (_, handles, source) = sealed_file(&sealer);
	assert!(source.read_legacy_filter(&handles[0]).is_err());
}

// ========== A file that begins with a header ==========

const TOY_HEADER: &[u8] = b"toy-header";

/// Starts a file with `TOY_HEADER` and a sealer that it shares with the test, and counts how many
/// times it was asked to. It can fail, and it can say that its header is not the length that it
/// has.
struct ToyStarter {
	sealer: Arc<ToySealer>,
	declared_len: usize,
	fail: bool,
	calls: Arc<AtomicUsize>,
}

impl ToyStarter {
	fn boxed(sealer: &Arc<ToySealer>) -> (Box<Self>, Arc<AtomicUsize>) {
		Self::with(sealer, TOY_HEADER.len(), false)
	}

	fn with(
		sealer: &Arc<ToySealer>,
		declared_len: usize,
		fail: bool,
	) -> (Box<Self>, Arc<AtomicUsize>) {
		let calls = Arc::new(AtomicUsize::new(0));
		let starter = Box::new(Self {
			sealer: Arc::clone(sealer),
			declared_len,
			fail,
			calls: Arc::clone(&calls),
		});
		(starter, calls)
	}
}

impl FileStarter for ToyStarter {
	fn header_len(&self) -> usize {
		self.declared_len
	}

	fn start(self: Box<Self>) -> Result<FileStart> {
		self.calls.fetch_add(1, Ordering::SeqCst);
		if self.fail {
			return Err(Error::Encryption(EncryptionError::KeyProvider(
				KeyProviderFailure::Unavailable,
			)));
		}
		let sealer: Arc<ToySealer> = Arc::clone(&self.sealer);
		Ok(FileStart {
			header: TOY_HEADER.to_vec(),
			sealer,
		})
	}
}

/// The table of `write_table` written into a file that begins with `TOY_HEADER`.
fn write_table_after_a_header(opts: &Arc<Options>, sealer: &Arc<ToySealer>) -> Vec<u8> {
	let mut file = Vec::new();
	{
		let (starter, _) = ToyStarter::boxed(sealer);
		let sink = BlockSink::deferred(&mut file, starter);
		let mut writer = TableWriter::with_sink(sink, 1, Arc::clone(opts), 0);
		for i in 0..KEYS {
			let key = InternalKey::new(
				format!("key{i:04}").into_bytes(),
				(i + 1) as u64,
				InternalKeyKind::Set,
			);
			writer.add(key, &value(i)).unwrap();
		}
		writer.finish().unwrap();
	}
	file
}

#[test]
fn a_sink_with_a_starter_writes_the_header_first_and_the_blocks_after_it() {
	let sealer = ToySealer::new(5);
	let (starter, calls) = ToyStarter::boxed(&sealer);
	let mut file = Vec::new();
	{
		let mut sink = BlockSink::deferred(&mut file, starter);

		// Nothing was asked of the starter and nothing was written, and the sink knows where the
		// first block goes.
		assert_eq!(sink.offset(), TOY_HEADER.len());
		assert_eq!(calls.load(Ordering::SeqCst), 0);

		// A block of 5 bytes is 21 once sealed, and its trailer is 9.
		let first =
			sink.write_block(BlockKind::Data, b"hello".to_vec(), CompressionType::None).unwrap();
		assert_eq!(calls.load(Ordering::SeqCst), 1);
		assert_eq!(first, BlockHandle::new(10, 21));
		assert_eq!(sink.offset(), 40);
		let second =
			sink.write_block(BlockKind::MetaIndex, Vec::new(), CompressionType::None).unwrap();
		assert_eq!(second, BlockHandle::new(40, 16));

		let size = sink.write_footer(&Footer::new(second, first)).unwrap();
		assert_eq!(size, 40 + 16 + SEALED_TRAILER_LEN + TABLE_FULL_FOOTER_LENGTH);
		assert_eq!(calls.load(Ordering::SeqCst), 1, "a file is started once");
	}

	assert_eq!(file.len(), 115);
	assert_eq!(&file[..TOY_HEADER.len()], TOY_HEADER);
	// The footer says that the blocks are sealed, whatever footer the caller gave the sink, and
	// holds the handles and nothing else.
	assert_eq!(file[65..], hex(&expected_footer_of(2, (40, 16), (10, 21))));
	assert_eq!(sealer.sealed(), vec![(BlockKind::Data, 10, 21), (BlockKind::MetaIndex, 40, 16)]);
}

#[test]
fn a_sink_that_is_not_written_to_does_not_start_its_file() {
	let sealer = ToySealer::new(5);
	let (starter, calls) = ToyStarter::boxed(&sealer);
	let mut file = Vec::new();
	drop(BlockSink::deferred(&mut file, starter));
	assert_eq!(calls.load(Ordering::SeqCst), 0);
	assert!(file.is_empty());
}

#[test]
fn a_file_without_a_block_is_started_by_its_footer() {
	let sealer = ToySealer::new(5);
	let (starter, calls) = ToyStarter::boxed(&sealer);
	let mut file = Vec::new();
	let size = {
		let mut sink = BlockSink::deferred(&mut file, starter);
		sink.write_footer(&Footer::new(BlockHandle::new(10, 0), BlockHandle::new(10, 0))).unwrap()
	};
	assert_eq!(calls.load(Ordering::SeqCst), 1);
	assert_eq!(size, TOY_HEADER.len() + TABLE_FULL_FOOTER_LENGTH);
	assert_eq!(file.len(), size);
	assert_eq!(&file[..TOY_HEADER.len()], TOY_HEADER);
	assert_eq!(file[10..], hex(&expected_footer_of(2, (10, 0), (10, 0))));
}

#[test]
fn a_starter_that_fails_ends_the_sink_and_nothing_is_written() {
	let sealer = ToySealer::new(5);
	let (starter, calls) = ToyStarter::with(&sealer, TOY_HEADER.len(), true);
	let mut file = Vec::new();
	{
		let mut sink = BlockSink::deferred(&mut file, starter);
		let err = sink
			.write_block(BlockKind::Data, b"hello".to_vec(), CompressionType::None)
			.unwrap_err();
		// The failure is the one the starter had, so a caller can tell a key that was not
		// available from damage.
		assert!(
			matches!(
				err,
				Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable))
			),
			"{err:?}"
		);
		assert_eq!(sink.offset(), TOY_HEADER.len());

		// The sink does not try again: a file is started once, and a failure ends it.
		assert!(sink
			.write_block(BlockKind::Data, b"hello".to_vec(), CompressionType::None)
			.is_err());
		let footer = Footer::new(BlockHandle::new(10, 0), BlockHandle::new(10, 0));
		assert!(sink.write_footer(&footer).is_err());
	}
	assert_eq!(calls.load(Ordering::SeqCst), 1);
	assert!(file.is_empty(), "nothing is written for a file that could not be started");
	assert!(sealer.sealed().is_empty());
}

#[test]
fn a_header_that_cannot_be_written_ends_the_sink_before_a_block_is_sealed() {
	let sealer = ToySealer::new(5);
	let (starter, _) = ToyStarter::boxed(&sealer);
	let mut writer = FailingWriter {
		written: Vec::new(),
		calls: 0,
		fail_on: 1,
	};
	{
		let mut sink = BlockSink::deferred(&mut writer, starter);
		assert!(sink
			.write_block(BlockKind::Data, b"hello".to_vec(), CompressionType::None)
			.is_err());
		assert!(sink
			.write_block(BlockKind::Data, b"hello".to_vec(), CompressionType::None)
			.is_err());
	}
	assert_eq!(writer.calls, 1, "no write was attempted after the one that failed");
	assert!(writer.written.is_empty());
	assert!(sealer.sealed().is_empty(), "no block was sealed in a file that has no header");
}

#[test]
fn a_header_that_is_not_as_long_as_it_was_said_to_be_is_refused() {
	// The first block goes where the sink was told, so a header of another length would put the
	// positions that blocks are sealed for and the offsets in their handles in different places.
	for declared in [TOY_HEADER.len() - 1, TOY_HEADER.len() + 1] {
		let sealer = ToySealer::new(5);
		let (starter, _) = ToyStarter::with(&sealer, declared, false);
		let mut file = Vec::new();
		{
			let mut sink = BlockSink::deferred(&mut file, starter);
			assert!(sink
				.write_block(BlockKind::Data, b"hello".to_vec(), CompressionType::None)
				.is_err());
		}
		assert!(file.is_empty(), "declared {declared}");
		assert!(sealer.sealed().is_empty(), "declared {declared}");
	}
}

/// The blocks of a sealed file, `(offset, size)` in file order, found from its end with nothing
/// but the trailers: the trailer before the footer says how long the last block is, which is
/// where the one before it ends, and so on up to the header. Every checksum is checked here.
fn extents_from_the_end(file: &[u8], header_len: usize) -> Vec<(usize, usize)> {
	let mut extents = Vec::new();
	let mut end = file.len() - TABLE_FULL_FOOTER_LENGTH;
	while end > header_len {
		let trailer = &file[end - SEALED_TRAILER_LEN..end];
		let len = u32::from_le_bytes(trailer[1..5].try_into().unwrap()) as usize;
		let start = (end - SEALED_TRAILER_LEN)
			.checked_sub(len)
			.expect("a block that is longer than the file");
		let stored = u32::from_le_bytes(trailer[5..9].try_into().unwrap());
		let crc = stored.wrapping_sub(0xa282_ead8).rotate_left(15);
		assert_eq!(crc32fast::hash(&file[start..end - 4]), crc, "checksum of the block at {start}");
		extents.push((start, len));
		end = start;
	}
	assert_eq!(end, header_len, "the first block starts where the header ends");
	extents.reverse();
	extents
}

#[test]
fn every_byte_of_a_file_with_a_header_belongs_to_the_header_a_block_or_the_footer() {
	for compression in [CompressionType::None, CompressionType::SnappyCompression] {
		let opts = table_options(compression);
		let sealer = ToySealer::new(0x33);
		let file = write_table_after_a_header(&opts, &sealer);

		// The header, then blocks that follow one another, then the footer.
		assert_eq!(&file[..TOY_HEADER.len()], TOY_HEADER);
		let extents = extents_from_the_end(&file, TOY_HEADER.len());
		let sealed = sealer.sealed();
		let mut ordered: Vec<(BlockKind, usize, usize)> =
			sealed.iter().map(|(kind, offset, len)| (*kind, *offset as usize, *len)).collect();
		ordered.sort_by_key(|(_, offset, _)| *offset);
		assert_eq!(
			ordered.iter().map(|(_, offset, len)| (*offset, *len)).collect::<Vec<_>>(),
			extents,
			"{compression:?}: the blocks that the sealer was asked for are the ones in the file"
		);
		let mut next = TOY_HEADER.len();
		for (offset, len) in &extents {
			assert_eq!(*offset, next);
			next = offset + len + SEALED_TRAILER_LEN;
		}
		assert_eq!(next, file.len() - TABLE_FULL_FOOTER_LENGTH);

		// All six kinds are there, and in the order that they are written in.
		let kinds: Vec<BlockKind> = ordered.iter().map(|(kind, ..)| *kind).collect();
		assert_eq!(kinds.first(), Some(&BlockKind::Data));
		assert_eq!(kinds[kinds.len() - 2..], [BlockKind::TopLevelIndex, BlockKind::MetaIndex]);
		for kind in ALL_KINDS {
			assert!(kinds.contains(&kind), "{compression:?}: no {kind:?} block");
		}
	}
}

#[test]
fn a_table_written_after_a_header_is_read_back() {
	for compression in [CompressionType::None, CompressionType::SnappyCompression] {
		let opts = table_options(compression);
		let sealer = ToySealer::new(0x33);
		let file = write_table_after_a_header(&opts, &sealer);
		let size = file.len() as u64;

		let (source, footer) =
			BlockSource::open_with(Arc::new(file), size, |_| Ok(sealed_by(&sealer))).unwrap();
		let source = source.after_header(TOY_HEADER.len() as u64);
		let table = Table::from_source(1, Arc::clone(&opts), size, source, footer).unwrap();
		for i in 0..KEYS {
			let (key, found) = table.get(&lookup(i)).unwrap().expect("the key is in the table");
			assert_eq!(key.user_key, format!("key{i:04}").into_bytes(), "{compression:?}");
			assert_eq!(found, value(i), "{compression:?}");
		}
	}
}

#[tokio::test]
async fn a_handle_that_names_bytes_of_the_header_is_refused() {
	let sealer = ToySealer::new(5);
	let (starter, _) = ToyStarter::boxed(&sealer);
	let mut file = Vec::new();
	let handle = {
		let mut sink = BlockSink::deferred(&mut file, starter);
		let handle = sink
			.write_block(BlockKind::Data, "abcdefgh".repeat(5).into_bytes(), CompressionType::None)
			.unwrap();
		sink.write_footer(&Footer::new(handle.clone(), handle.clone())).unwrap();
		handle
	};
	assert_eq!(handle.offset, TOY_HEADER.len());
	let size = file.len() as u64;
	let source = BlockSource::with_protection(Arc::new(file), size, sealed_by(&sealer))
		.after_header(TOY_HEADER.len() as u64);
	let comparator = Arc::clone(&Options::default().internal_comparator);

	// The block is where its handle says.
	assert_eq!(
		source.read_contents(BlockKind::Data, &handle).unwrap(),
		"abcdefgh".repeat(5).into_bytes()
	);

	// A handle at any byte of the header, or that starts in it and ends in the first block, is not
	// a block, and the sealer is not asked to open it.
	let asked = sealer.opened().len();
	for offset in [0, 1, TOY_HEADER.len() - 1] {
		let forged = BlockHandle::new(offset, handle.size);
		let err = source.read_contents(BlockKind::Data, &forged).unwrap_err();
		assert!(
			matches!(
				err,
				Error::SSTable(SSTableError::BlockHandleOutOfBounds {
					first_block: 10,
					..
				})
			),
			"offset {offset}: {err:?}"
		);
		let err = source
			.read_block_async(BlockKind::Data, &forged, Arc::clone(&comparator))
			.await
			.err()
			.expect("a handle into the header is an error");
		assert!(
			matches!(err, Error::SSTable(SSTableError::BlockHandleOutOfBounds { .. })),
			"offset {offset}: {err:?}"
		);
	}
	assert_eq!(sealer.opened().len(), asked);
}

// ========== The footer of a sealed table ==========

/// The footer decoder as it was before there were sealed tables, kept as it was: a table that is
/// written now has to be refused by the reader that exists in the field, which is that one. It
/// returns the format and the checksum type as the bytes that it accepted, and the handles.
fn frozen_footer_decode(buf: &[u8]) -> Result<(u8, u8, BlockHandle, BlockHandle)> {
	const MAGIC: [u8; 8] = [0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb];
	const FOOTER_LENGTH: usize = 42;

	if buf.len() < FOOTER_LENGTH {
		return Err(Error::from(SSTableError::FooterTooShort {
			footer_len: buf.len(),
		}));
	}
	let magic = &buf[buf.len() - MAGIC.len()..];
	if magic != MAGIC {
		return Err(Error::from(SSTableError::BadMagicNumber {
			magic: magic.to_vec(),
		}));
	}
	let format = match buf[0] {
		1 => 1,
		_ => return Err(Error::InvalidTableFormat),
	};
	let checksum = match buf[1] {
		1 => 1,
		_ => {
			return Err(Error::from(SSTableError::InvalidChecksumType {
				checksum_type: buf[1],
			}))
		}
	};
	let (meta_index, metalen) = BlockHandle::decode(&buf[2..])?;
	if metalen == 0 {
		return Err(Error::from(SSTableError::BadMetaIndexBlockHandle));
	}
	let (index, _) = BlockHandle::decode(&buf[2 + metalen..])?;
	Ok((format, checksum, meta_index, index))
}

fn footer_of(file: &[u8]) -> &[u8] {
	&file[file.len() - TABLE_FULL_FOOTER_LENGTH..]
}

#[test]
fn a_reader_that_predates_sealed_tables_refuses_their_footer() {
	let opts = table_options(CompressionType::None);
	let sealer = ToySealer::new(5);
	let sealed = write_table_after_a_header(&opts, &sealer);
	let plain = write_table(&opts, Protection::Plain);

	// The control: the old decoder reads the footer of a plain table.
	let (format, checksum, ..) = frozen_footer_decode(footer_of(&plain)).unwrap();
	assert_eq!((format, checksum), (1, 1));
	assert_eq!(Footer::decode(footer_of(&plain)).unwrap().checksum, ChecksumType::CRC32c);

	// The sealed one is an error and not a table with checksums that fail, and the error is
	// raised by the footer, before anything else of the file is looked at.
	let result = frozen_footer_decode(footer_of(&sealed));
	assert!(
		matches!(
			result,
			Err(Error::SSTable(SSTableError::InvalidChecksumType {
				checksum_type: 2
			}))
		),
		"{result:?}"
	);
	let footer = Footer::decode(footer_of(&sealed)).unwrap();
	assert_eq!(footer.checksum, ChecksumType::SealedCRC32c);
	assert_eq!(footer.checksum as u8, 2);
}

/// Reads of a file, as `(offset, length)` in the order that they were asked for.
pub(crate) struct RecordingFile {
	bytes: Vec<u8>,
	reads: Mutex<Vec<(u64, usize)>>,
}

impl RecordingFile {
	pub(crate) fn new(bytes: Vec<u8>) -> Arc<Self> {
		Arc::new(Self {
			bytes,
			reads: Mutex::new(Vec::new()),
		})
	}

	pub(crate) fn reads(&self) -> Vec<(u64, usize)> {
		self.reads.lock().unwrap().clone()
	}

	/// The file as what a table is opened over.
	pub(crate) fn shared(self: &Arc<Self>) -> Arc<dyn File> {
		Arc::<Self>::clone(self)
	}
}

impl File for RecordingFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.reads.lock().unwrap().push((offset, buf.len()));
		self.bytes.read_at(offset, buf)
	}

	fn size(&self) -> Result<u64> {
		self.bytes.size()
	}
}

#[test]
fn a_sealed_table_is_refused_by_the_opener_that_has_no_key_after_reading_only_its_footer() {
	let opts = table_options(CompressionType::None);
	let sealer = ToySealer::new(5);
	let sealed = write_table_after_a_header(&opts, &sealer);
	let size = sealed.len() as u64;

	let file = RecordingFile::new(sealed);
	let result = Table::new(1, Arc::clone(&opts), file.shared(), size);
	assert!(
		matches!(result, Err(Error::Encryption(EncryptionError::NotConfigured))),
		"{:?}",
		result.err()
	);
	assert_eq!(
		file.reads(),
		vec![(size - TABLE_FULL_FOOTER_LENGTH as u64, TABLE_FULL_FOOTER_LENGTH)]
	);
	assert!(sealer.opened().is_empty());

	let result = BlockSource::open(file, size);
	assert!(matches!(result, Err(Error::Encryption(EncryptionError::NotConfigured))));
}
