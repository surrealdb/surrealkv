//! Tests of the trailer layouts, of `BlockSink` and `BlockSource`, and of what they do with a
//! sealer.
//!
//! There is no real sealer yet. `ToySealer` is one that is good enough to tell the tests whether
//! the seam gives a sealer what it must be given, and whether a block that does not open is
//! stopped before it is decompressed. It is not a cipher.

use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::sstable::block::BlockHandle;
use crate::sstable::block_io::{
	verify_trailer,
	BlockContext,
	BlockKind,
	BlockSealer,
	BlockSink,
	BlockSource,
	Protection,
	Trailer,
	TrailerLayout,
	PLAIN_TRAILER_LEN,
	SEALED_TRAILER_LEN,
};
use crate::sstable::error::SSTableError;
use crate::sstable::table::{mask, Footer, Table, TableWriter, TABLE_FULL_FOOTER_LENGTH};
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
/// top-level index is the handle `(0, 5)`: format 1, checksum 1, the handles as varints, zeros
/// up to byte 42 and the magic number.
fn expected_footer(meta_offset: u8) -> String {
	format!("0101{meta_offset:02x}000005{}57fb808b247547db", "00".repeat(36))
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
		[hex("68656c6c6f"), hex("00b7980055"), hex("00dd8e9e81"), hex(&expected_footer(10))]
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
		hex(&expected_footer(14)),
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
