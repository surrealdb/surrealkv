//! Tests of sealed SSTables, written and read with the real ciphers.
//!
//! What the writer leaves in a file is checked without the reader that the engine uses: the
//! blocks are found from the end of the file by their trailers and opened with the cipher that the
//! header of the file names, trying each kind of block, so that what a block is bound to is
//! checked against what is said in the format and not against the code that wrote it.
//!
//! Tables here are small, since sealing in an unoptimised build is slow.

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::error::{EncryptionError, Error, KeyProviderFailure};
use crate::sstable::block::BlockHandle;
use crate::sstable::block_io::{
	BlockContext,
	BlockKind,
	BlockSealer,
	BlockSink,
	BlockSource,
	FileStart,
	FileStarter,
	Trailer,
	TrailerLayout,
};
use crate::sstable::sealed::SstSealer;
use crate::sstable::table::{
	decompress_block,
	ChecksumType,
	Footer,
	IndexType,
	Table,
	TableFilter,
	TableWriter,
	TABLE_FULL_FOOTER_LENGTH,
};
use crate::tde::{
	self,
	CipherSuite,
	CryptoHeader,
	DbId,
	EncryptionContext,
	FileKind,
	KeyManager,
	KeyState,
	SoftwareKeyManager,
	Zeroizing,
	HEADER_LEN,
	ROOT_KEY_LEN,
};
use crate::test::block_io_tests::{extents_from_the_end, RecordingFile};
use crate::{
	CompressionType,
	InternalKey,
	InternalKeyKind,
	InternalKeyRange,
	LSMIterator,
	Options,
	INTERNAL_KEY_SEQ_NUM_MAX,
};

const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];
const COMPRESSIONS: [CompressionType; 3] =
	[CompressionType::None, CompressionType::SnappyCompression, CompressionType::ZstdCompression];

/// The id of the tables that these tests write, and the number of keys of the larger ones.
const TABLE_ID: u64 = 7;
const KEYS: usize = 120;

// ========== Contexts, options and tables ==========

fn db_id() -> DbId {
	DbId::from_bytes([0xD5; 16])
}

fn manager() -> Arc<SoftwareKeyManager> {
	Arc::new(SoftwareKeyManager::new([0x42; ROOT_KEY_LEN]))
}

fn context_for(key_manager: Arc<dyn KeyManager>, suite: CipherSuite) -> Arc<EncryptionContext> {
	Arc::new(EncryptionContext::new(key_manager, db_id(), suite))
}

fn context(suite: CipherSuite) -> Arc<EncryptionContext> {
	context_for(manager(), suite)
}

/// Options for tables of a few blocks of each kind, sealed if a context is given.
fn options(
	context: Option<&Arc<EncryptionContext>>,
	compression: CompressionType,
	filter: bool,
) -> Arc<Options> {
	let mut opts = Options {
		block_size: 256,
		block_restart_interval: 4,
		index_partition_size: 100,
		compression_per_level: vec![compression],
		..Default::default()
	};
	if !filter {
		opts.filter_policy = None;
	}
	if let Some(context) = context {
		opts = opts.with_encryption_context(Arc::clone(context));
	}
	Arc::new(opts)
}

/// What is written to a table: points in order, and range deletions.
#[derive(Clone, Default)]
struct Input {
	points: Vec<(InternalKey, Vec<u8>)>,
	range_deletions: Vec<(Vec<u8>, Vec<u8>, u64)>,
}

fn key_of(i: usize) -> Vec<u8> {
	format!("key{i:04}").into_bytes()
}

fn value_of(i: usize) -> Vec<u8> {
	format!("value{i:04}-{}", "ab".repeat(20)).into_bytes()
}

fn point(i: usize) -> (InternalKey, Vec<u8>) {
	(InternalKey::new(key_of(i), (i + 1) as u64, InternalKeyKind::Set), value_of(i))
}

fn many_points(n: usize) -> Input {
	Input {
		points: (0..n).map(point).collect(),
		range_deletions: Vec::new(),
	}
}

/// Writes `input` as table `id` through `TableWriter::new`, which is what the engine does, and
/// returns the file.
fn write_table(opts: &Arc<Options>, id: u64, input: &Input) -> Vec<u8> {
	let mut buf = Vec::new();
	let size = {
		let mut writer = TableWriter::new(&mut buf, id, Arc::clone(opts), 0);
		for (key, value) in &input.points {
			writer.add(key.clone(), value).unwrap();
		}
		for (start, end, seq) in &input.range_deletions {
			writer.add_range_deletion(start.clone(), end.clone(), *seq);
		}
		writer.finish().unwrap()
	};
	assert_eq!(size, buf.len(), "finish() must report the size of the file");
	buf
}

fn footer_of(file: &[u8]) -> Footer {
	Footer::decode(&file[file.len() - TABLE_FULL_FOOTER_LENGTH..]).unwrap()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
	haystack.windows(needle.len()).any(|window| window == needle)
}

// ========== Opening a file without the reader of the engine ==========

const SST_KINDS: [tde::BlockKind; 6] = [
	tde::BlockKind::SstData,
	tde::BlockKind::SstIndexPartition,
	tde::BlockKind::SstTopLevelIndex,
	tde::BlockKind::SstFilterPartition,
	tde::BlockKind::SstFilterIndex,
	tde::BlockKind::SstMetaIndex,
];

/// A block of a sealed file, opened.
struct Unit {
	kind: tde::BlockKind,
	offset: usize,
	/// The length of the sealed block, as the trailer says it.
	sealed_len: usize,
	/// The byte of the trailer that says how the block was compressed.
	compression: u8,
	/// The block as it was before it was sealed, which is still compressed.
	contents: Vec<u8>,
}

impl Unit {
	/// The block as it was before it was compressed.
	fn decompressed(&self) -> Vec<u8> {
		decompress_block(&self.contents, CompressionType::try_from(self.compression).unwrap())
			.unwrap()
	}
}

/// Every block of `file`, which is the sealed table `id` of the database of `context`, in file
/// order. The blocks are found by their trailers, from the end, and the kind of each is the one
/// that it opens under: a block opens under exactly one.
fn open_units(file: &[u8], context: &EncryptionContext, id: u64) -> Vec<Unit> {
	let cipher = context
		.open_file_from_bytes(&file[..HEADER_LEN], FileKind::Sst, id, &[])
		.expect("the header of the file opens under its key");
	extents_from_the_end(file, HEADER_LEN)
		.into_iter()
		.map(|(offset, len)| {
			let compression = file[offset + len];
			let mut opened: Vec<Unit> = SST_KINDS
				.iter()
				.filter_map(|&kind| {
					let mut contents = file[offset..offset + len].to_vec();
					let at = tde::BlockContext::new(kind, offset as u64, compression);
					let plain_len = cipher.open_suffix(&at, &mut contents).ok()?.len();
					contents.truncate(plain_len);
					Some(Unit {
						kind,
						offset,
						sealed_len: len,
						compression,
						contents,
					})
				})
				.collect();
			assert_eq!(opened.len(), 1, "the block at {offset} opens under exactly one kind");
			opened.remove(0)
		})
		.collect()
}

// ========== What the writer leaves in a file ==========

#[test]
fn a_sealed_table_begins_with_the_header_of_its_file_and_its_footer_says_that_it_is_sealed() {
	for suite in SUITES {
		let ctx = context(suite);
		let opts = options(Some(&ctx), CompressionType::None, true);
		let file = write_table(&opts, TABLE_ID, &many_points(KEYS));

		let header = CryptoHeader::decode(&file[..HEADER_LEN]).unwrap();
		assert_eq!(header.identity.suite, suite);
		assert_eq!(header.identity.file_kind, FileKind::Sst);
		assert_eq!(header.identity.file_id, TABLE_ID);
		assert_eq!(header.identity.key_id, 1);
		assert_eq!(header.identity.db_id, db_id());
		ctx.open_file(&header, FileKind::Sst, TABLE_ID, &[]).expect("the header is authentic");

		assert_eq!(footer_of(&file).checksum, ChecksumType::SealedCRC32c, "{suite:?}");
		// Blocks follow one another from the header to the footer: this walk fails otherwise.
		let extents = extents_from_the_end(&file, HEADER_LEN);
		assert!(extents.len() > 6, "{suite:?}: {} blocks", extents.len());
		assert_eq!(extents[0].0, HEADER_LEN);
	}
}

#[test]
fn a_table_without_a_context_is_written_as_it_always_was() {
	let opts = options(None, CompressionType::None, true);
	let file = write_table(&opts, TABLE_ID, &many_points(KEYS));
	assert_eq!(footer_of(&file).checksum, ChecksumType::CRC32c);
	assert!(!CryptoHeader::has_magic(&file), "a plain table has no header");
}

#[test]
fn every_block_of_every_kind_is_sealed_for_its_kind_and_its_place() {
	for suite in SUITES {
		let ctx = context(suite);
		let opts = options(Some(&ctx), CompressionType::None, true);
		let input = many_points(KEYS);
		let file = write_table(&opts, TABLE_ID, &input);
		let units = open_units(&file, &ctx, TABLE_ID);

		// All six kinds, and in the order that they are written in: data blocks as they fill, then
		// the filter, then the index, then the meta-index.
		let kinds: Vec<tde::BlockKind> = units.iter().map(|unit| unit.kind).collect();
		let count = |wanted: tde::BlockKind| kinds.iter().filter(|kind| **kind == wanted).count();
		for kind in SST_KINDS {
			assert!(count(kind) > 0, "{suite:?}: no block of kind {kind:?}");
		}
		assert!(count(tde::BlockKind::SstData) > 5);
		assert_eq!(count(tde::BlockKind::SstFilterIndex), 1);
		assert_eq!(count(tde::BlockKind::SstTopLevelIndex), 1);
		assert_eq!(count(tde::BlockKind::SstMetaIndex), 1);
		assert_eq!(kinds.first(), Some(&tde::BlockKind::SstData));
		assert_eq!(
			kinds[kinds.len() - 2..],
			[tde::BlockKind::SstTopLevelIndex, tde::BlockKind::SstMetaIndex]
		);

		// Sealing adds the tag and nothing else, and no position is sealed twice.
		for unit in &units {
			assert_eq!(unit.sealed_len, unit.contents.len() + tde::TAG_LEN);
		}
		assert!(units.windows(2).all(|pair| pair[0].offset < pair[1].offset));

		// What was sealed is the blocks of the table: every key and value is in the data blocks
		// that were opened, and the meta-index holds the smallest and the largest key.
		let data: Vec<u8> = units
			.iter()
			.filter(|unit| unit.kind == tde::BlockKind::SstData)
			.flat_map(|unit| unit.contents.clone())
			.collect();
		for i in 0..KEYS {
			assert!(contains(&data, &value_of(i)), "{suite:?}: the value of key {i}");
		}
		let meta = units.last().unwrap();
		assert!(contains(&meta.contents, &key_of(0)));
		assert!(contains(&meta.contents, &key_of(KEYS - 1)));
	}
}

#[test]
fn a_block_is_compressed_and_then_sealed_with_its_compression_bound() {
	for compression in COMPRESSIONS {
		let ctx = context(CipherSuite::Aes256Gcm);
		let opts = options(Some(&ctx), compression, true);
		let file = write_table(&opts, TABLE_ID, &many_points(40));
		let units = open_units(&file, &ctx, TABLE_ID);

		for unit in &units {
			// The data and the index are compressed as the options say, and the other kinds never
			// are. Whatever the byte is, it is what the block was sealed with: the units opened.
			let expected = match unit.kind {
				tde::BlockKind::SstData
				| tde::BlockKind::SstIndexPartition
				| tde::BlockKind::SstTopLevelIndex => compression as u8,
				_ => CompressionType::None as u8,
			};
			assert_eq!(unit.compression, expected, "{compression:?}: {:?}", unit.kind);
		}

		// What was sealed is the compressed block, which takes the decompressor that the byte
		// names to get back the block.
		let first = &units[0];
		assert_eq!(first.kind, tde::BlockKind::SstData);
		assert!(contains(&first.decompressed(), &key_of(0)), "{compression:?}");
	}
}

#[test]
fn a_block_does_not_open_under_a_compression_byte_that_it_was_not_sealed_with() {
	// The byte is outside the sealed bytes, where anyone can change it, which is why it is bound.
	let ctx = context(CipherSuite::XChaCha20Poly1305);
	let opts = options(Some(&ctx), CompressionType::SnappyCompression, true);
	let file = write_table(&opts, TABLE_ID, &many_points(40));
	let cipher =
		ctx.open_file_from_bytes(&file[..HEADER_LEN], FileKind::Sst, TABLE_ID, &[]).unwrap();

	let (offset, len) = extents_from_the_end(&file, HEADER_LEN)[0];
	let sealed = file[offset..offset + len].to_vec();
	let at =
		|compression| tde::BlockContext::new(tde::BlockKind::SstData, offset as u64, compression);
	assert_eq!(file[offset + len], 1);
	assert!(cipher.open_suffix(&at(1), &mut sealed.clone()).is_ok());
	for other in [0u8, 2, 0x81] {
		let mut copy = sealed.clone();
		let result = cipher.open_suffix(&at(other), &mut copy);
		assert_eq!(result.unwrap_err(), EncryptionError::AuthenticationFailed, "byte {other}");
	}
}

// ========== The canary ==========

/// Bytes that must be in a plain table and in no byte of a sealed one: keys that no neighbour
/// shares a first byte with, so that no block stores them in a shortened form, values, and the
/// bounds of a range deletion, which only the meta-index holds.
const MARKERS: [&str; 10] = [
	"a-canary-smallest-7e2c91",
	"h-canary-middle-one-c41d77",
	"j-canary-middle-two-1f66d0",
	"z-canary-largest-b90e12",
	"canary-value-for-smallest-0d44ab",
	"canary-value-for-h-8c1e5f",
	"canary-value-for-j-35e9c2",
	"canary-value-for-z-f10a98",
	"m-canary-range-start-3f7b66",
	"q-canary-range-end-d2c819",
];

fn canary_input() -> Input {
	let mut keys: Vec<(String, String)> = vec![
		(MARKERS[0].into(), MARKERS[4].into()),
		(MARKERS[1].into(), MARKERS[5].into()),
		(MARKERS[2].into(), MARKERS[6].into()),
		(MARKERS[3].into(), MARKERS[7].into()),
	];
	// Neighbours with another first byte each, between the markers.
	keys.extend((0..40).map(|i| (format!("f-filler-key-{i:03}"), format!("filler-value-{i:03}"))));
	keys.sort();

	Input {
		points: keys
			.into_iter()
			.enumerate()
			.map(|(i, (key, value))| {
				(
					InternalKey::new(key.into_bytes(), (i + 1) as u64, InternalKeyKind::Set),
					value.into_bytes(),
				)
			})
			.collect(),
		range_deletions: vec![(MARKERS[8].into(), MARKERS[9].into(), 500)],
	}
}

#[test]
fn a_plain_table_holds_every_marker_which_is_what_makes_their_absence_from_a_sealed_one_mean_something(
) {
	let opts = options(None, CompressionType::None, true);
	let file = write_table(&opts, TABLE_ID, &canary_input());
	for marker in MARKERS {
		assert!(contains(&file, marker.as_bytes()), "the plain table does not hold {marker}");
	}
}

#[test]
fn no_marker_is_anywhere_in_a_sealed_table_but_every_one_is_in_it_when_it_is_opened() {
	for suite in SUITES {
		for compression in COMPRESSIONS {
			let ctx = context(suite);
			let opts = options(Some(&ctx), compression, true);
			let file = write_table(&opts, TABLE_ID, &canary_input());

			for marker in MARKERS {
				assert!(
					!contains(&file, marker.as_bytes()),
					"{suite:?}, {compression:?}: the sealed table holds {marker}"
				);
			}
			// Not even the part that the markers share, in the header, a block or the footer.
			assert!(!contains(&file, b"canary"), "{suite:?}, {compression:?}");
			assert!(!contains(&file, b"filler"), "{suite:?}, {compression:?}");

			// And they are there, sealed: the table was not emptied of them.
			let opened: Vec<u8> = open_units(&file, &ctx, TABLE_ID)
				.iter()
				.flat_map(|unit| unit.decompressed())
				.collect();
			for marker in MARKERS {
				assert!(
					contains(&opened, marker.as_bytes()),
					"{suite:?}, {compression:?}: the opened table does not hold {marker}"
				);
			}
		}
	}
}

#[test]
fn the_bounds_of_a_table_are_sealed_with_the_rest_of_its_meta_block() {
	// A table of range deletions alone: nothing but the meta-index holds the bounds.
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(Some(&ctx), CompressionType::None, true);
	let input = Input {
		points: Vec::new(),
		range_deletions: vec![(MARKERS[8].into(), MARKERS[9].into(), 500)],
	};
	let file = write_table(&opts, TABLE_ID, &input);
	assert!(!contains(&file, MARKERS[8].as_bytes()));
	assert!(!contains(&file, MARKERS[9].as_bytes()));

	let units = open_units(&file, &ctx, TABLE_ID);
	let meta = units.last().unwrap();
	assert_eq!(meta.kind, tde::BlockKind::SstMetaIndex);
	assert!(contains(&meta.contents, MARKERS[8].as_bytes()));
	assert!(contains(&meta.contents, MARKERS[9].as_bytes()));
}

// ========== Salts ==========

#[test]
fn a_table_written_again_under_the_same_id_is_another_file_with_another_key() {
	for suite in SUITES {
		let ctx = context(suite);
		let opts = options(Some(&ctx), CompressionType::None, true);
		let input = many_points(KEYS);
		let first = write_table(&opts, TABLE_ID, &input);
		let second = write_table(&opts, TABLE_ID, &input);

		// The same identity, with a salt and a tag of its own.
		let (a, b) = (
			CryptoHeader::decode(&first[..HEADER_LEN]).unwrap(),
			CryptoHeader::decode(&second[..HEADER_LEN]).unwrap(),
		);
		assert_eq!(a.identity, b.identity);
		assert_ne!(a.salt, b.salt, "{suite:?}");
		assert_ne!(a.tag, b.tag, "{suite:?}");

		// The same blocks at the same places: only the key tells the two files apart, and the
		// same bytes sealed at the same place under it are other bytes.
		let (first_extents, second_extents) =
			(extents_from_the_end(&first, HEADER_LEN), extents_from_the_end(&second, HEADER_LEN));
		assert_eq!(first_extents, second_extents);
		let (offset, len) = first_extents[0];
		assert_ne!(first[offset..offset + len], second[offset..offset + len], "{suite:?}");

		// And a block of one does not open in the other.
		let first_cipher =
			ctx.open_file_from_bytes(&first[..HEADER_LEN], FileKind::Sst, TABLE_ID, &[]).unwrap();
		let mut moved = second[offset..offset + len].to_vec();
		let at = tde::BlockContext::new(tde::BlockKind::SstData, offset as u64, 0);
		assert_eq!(
			first_cipher.open_suffix(&at, &mut moved).unwrap_err(),
			EncryptionError::AuthenticationFailed,
			"{suite:?}"
		);
	}
}

// ========== Taking the key ==========

/// A key manager that counts the roots it is asked for, and that can be made to fail.
struct FlakyManager {
	inner: SoftwareKeyManager,
	unavailable: AtomicBool,
	roots: AtomicUsize,
}

impl FlakyManager {
	fn new() -> Arc<Self> {
		Arc::new(Self {
			inner: SoftwareKeyManager::new([0x42; ROOT_KEY_LEN]),
			unavailable: AtomicBool::new(false),
			roots: AtomicUsize::new(0),
		})
	}

	fn roots_asked_for(&self) -> usize {
		self.roots.load(Ordering::SeqCst)
	}

	fn set_unavailable(&self, unavailable: bool) {
		self.unavailable.store(unavailable, Ordering::SeqCst);
	}
}

impl KeyManager for FlakyManager {
	fn active_key_id(&self) -> u32 {
		self.inner.active_key_id()
	}

	fn key_state(&self, key_id: u32) -> KeyState {
		self.inner.key_state(key_id)
	}

	fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
		self.roots.fetch_add(1, Ordering::SeqCst);
		if self.unavailable.load(Ordering::SeqCst) {
			return Err(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable));
		}
		self.inner.root_key(key_id)
	}
}

fn is_unavailable(err: &Error) -> bool {
	matches!(err, Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable)))
}

#[test]
fn the_key_of_a_table_is_taken_when_its_first_block_is_written_and_not_before() {
	let km = FlakyManager::new();
	let ctx = context_for(Arc::<FlakyManager>::clone(&km), CipherSuite::Aes256Gcm);
	let opts = options(Some(&ctx), CompressionType::None, true);

	// A writer that is never written to takes no key and writes nothing.
	let mut unused = Vec::new();
	drop(TableWriter::new(&mut unused, TABLE_ID, Arc::clone(&opts), 0));
	assert_eq!(km.roots_asked_for(), 0);
	assert!(unused.is_empty());

	// One entry does not fill a block, so nothing is written until the table is finished.
	let mut file = Vec::new();
	let mut writer = TableWriter::new(&mut file, TABLE_ID, Arc::clone(&opts), 0);
	let (key, value) = point(0);
	writer.add(key, &value).unwrap();
	assert_eq!(km.roots_asked_for(), 0);
	writer.finish().unwrap();
	assert_eq!(km.roots_asked_for(), 1, "a table has one key");
	assert_eq!(footer_of(&file).checksum, ChecksumType::SealedCRC32c);
}

#[test]
fn a_key_that_cannot_be_had_is_the_error_of_the_call_that_writes_and_nothing_is_written() {
	let km = FlakyManager::new();
	let ctx = context_for(Arc::<FlakyManager>::clone(&km), CipherSuite::XChaCha20Poly1305);
	let opts = options(Some(&ctx), CompressionType::None, true);
	km.set_unavailable(true);

	// The error is that of `finish`, when no block was written before it.
	let mut file = Vec::new();
	let mut writer = TableWriter::new(&mut file, TABLE_ID, Arc::clone(&opts), 0);
	let (key, value) = point(0);
	writer.add(key, &value).unwrap();
	let err = writer.finish().unwrap_err();
	assert!(is_unavailable(&err), "{err:?}");
	assert!(file.is_empty(), "a table that could not be started has no byte");

	// Or that of the `add` that fills the first block.
	let mut file = Vec::new();
	let mut failure = None;
	{
		let mut writer = TableWriter::new(&mut file, TABLE_ID, Arc::clone(&opts), 0);
		for i in 0..KEYS {
			let (key, value) = point(i);
			if let Err(e) = writer.add(key, &value) {
				failure = Some(e);
				break;
			}
		}
	}
	let err = failure.expect("one of the entries filled a block");
	assert!(is_unavailable(&err), "{err:?}");
	assert!(file.is_empty());

	// The provider comes back, and the same table is written, as a retry would.
	km.set_unavailable(false);
	let file = write_table(&opts, TABLE_ID, &many_points(KEYS));
	assert!(CryptoHeader::has_magic(&file));
	open_units(&file, &ctx, TABLE_ID);
}

/// A manager whose active key is in a state that cannot start a file, with a root for every id.
struct StuckManager(KeyState);

impl KeyManager for StuckManager {
	fn active_key_id(&self) -> u32 {
		1
	}

	fn key_state(&self, _key_id: u32) -> KeyState {
		self.0
	}

	fn root_key(&self, _key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
		Ok(Zeroizing::new([0x66; ROOT_KEY_LEN]))
	}
}

#[test]
fn a_table_is_not_started_under_a_key_that_may_no_longer_be_written_with() {
	for state in [KeyState::DecryptOnly, KeyState::Retired, KeyState::Unknown] {
		let ctx = context_for(Arc::new(StuckManager(state)), CipherSuite::Aes256Gcm);
		let opts = options(Some(&ctx), CompressionType::None, true);

		let mut file = Vec::new();
		let mut writer = TableWriter::new(&mut file, TABLE_ID, opts, 0);
		let (key, value) = point(0);
		writer.add(key, &value).unwrap();
		let err = writer.finish().unwrap_err();
		let expected = if state == KeyState::Unknown {
			EncryptionError::UnknownKeyId
		} else {
			EncryptionError::KeyRetired
		};
		assert!(
			matches!(&err, Error::Encryption(found) if *found == expected),
			"{state:?}: {err:?}"
		);
		assert!(file.is_empty(), "{state:?}: a table was started under a key that cannot write");
	}
}

// ========== One cipher, one file ==========

/// Hands the same sealer to every file that it starts, which nothing in the engine does and which
/// the cipher refuses to let a second file use.
struct SharedStarter {
	header: Vec<u8>,
	sealer: Arc<SstSealer>,
}

impl FileStarter for SharedStarter {
	fn header_len(&self) -> usize {
		self.header.len()
	}

	fn start(self: Box<Self>) -> crate::error::Result<FileStart> {
		let sealer: Arc<SstSealer> = Arc::clone(&self.sealer);
		Ok(FileStart {
			header: self.header.clone(),
			sealer,
		})
	}
}

#[test]
fn a_sealer_given_to_a_second_writer_is_refused_the_positions_that_the_first_one_used() {
	for suite in SUITES {
		let ctx = context(suite);
		let (header, cipher) = ctx.create_file(FileKind::Sst, TABLE_ID, &[]).unwrap();
		let sealer = Arc::new(SstSealer::new(cipher));
		let opts = options(None, CompressionType::None, true);

		let write = |file: &mut Vec<u8>| -> crate::error::Result<usize> {
			let starter = Box::new(SharedStarter {
				header: header.encode().to_vec(),
				sealer: Arc::clone(&sealer),
			});
			let sink = BlockSink::deferred(file, starter);
			let mut writer = TableWriter::with_sink(sink, TABLE_ID, Arc::clone(&opts), 0);
			// Three entries do not fill a block, so the first block is written by `finish`.
			for i in 0..3 {
				let (key, value) = point(i);
				writer.add(key, &value)?;
			}
			writer.finish()
		};

		let (mut first, mut second) = (Vec::new(), Vec::new());
		write(&mut first).unwrap();
		let err = write(&mut second).unwrap_err();
		assert!(matches!(err, Error::Encryption(EncryptionError::PositionReuse)), "{err:?}");
		// The second file has its header and not one byte of a block: the position was refused
		// before anything was sealed or written at it.
		assert_eq!(second.len(), HEADER_LEN, "{suite:?}");
		assert_eq!(second, first[..HEADER_LEN]);
	}
}

#[test]
fn a_sealer_of_a_table_that_was_read_cannot_seal() {
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(Some(&ctx), CompressionType::None, true);
	let file = write_table(&opts, TABLE_ID, &many_points(10));
	let cipher =
		ctx.open_file_from_bytes(&file[..HEADER_LEN], FileKind::Sst, TABLE_ID, &[]).unwrap();
	let sealer: Arc<dyn BlockSealer> = Arc::new(SstSealer::new(cipher));

	let at = BlockContext {
		kind: BlockKind::Data,
		offset: 10_000,
		compression: 0,
	};
	let mut block = b"bytes".to_vec();
	let err = sealer.seal(&at, &mut block).unwrap_err();
	assert!(matches!(err, Error::Encryption(EncryptionError::InvalidInput(_))), "{err:?}");
	assert_eq!(block, b"bytes", "a refused seal leaves the block as it was");
}

// ========== Reading a sealed table ==========

fn lookup(i: usize) -> InternalKey {
	InternalKey::new(key_of(i), INTERNAL_KEY_SEQ_NUM_MAX, InternalKeyKind::Set)
}

/// Opens `file` as table `id` of the database that `opts` belong to, as the engine does.
fn open_table(opts: &Arc<Options>, file: &[u8], id: u64) -> crate::error::Result<Table> {
	Table::new(id, Arc::clone(opts), Arc::new(file.to_vec()), file.len() as u64)
}

fn encryption_error(result: crate::error::Result<Table>) -> EncryptionError {
	match result {
		Err(Error::Encryption(err)) => err,
		Err(other) => panic!("expected an encryption error, got {other:?}"),
		Ok(_) => panic!("the table was opened"),
	}
}

/// The `(user key, value)` entries that a scan of `table` over `range` returns, forwards.
fn scan(table: &Table, range: Option<InternalKeyRange>) -> Vec<(Vec<u8>, Vec<u8>)> {
	let mut iter = table.iter(range).unwrap();
	iter.seek_first().unwrap();
	let mut found = Vec::new();
	while iter.valid() {
		found.push((iter.key().user_key().to_vec(), iter.value_encoded().unwrap().to_vec()));
		iter.next().unwrap();
	}
	found
}

#[test]
fn a_sealed_table_is_read_back_whatever_its_suite_its_compression_and_its_filter() {
	for suite in SUITES {
		for compression in COMPRESSIONS {
			for filter in [true, false] {
				let what = format!("{suite:?}, {compression:?}, filter {filter}");
				let ctx = context(suite);
				let opts = options(Some(&ctx), compression, filter);
				let file = write_table(&opts, TABLE_ID, &many_points(KEYS));
				let table = open_table(&opts, &file, TABLE_ID).unwrap();

				// The table has more than one partition of the index and of the filter, so that
				// what is read below is more than one block of each kind.
				let IndexType::Partitioned(index) = &table.index_block;
				assert!(index.blocks.len() > 2, "{what}");
				match (&table.filter, filter) {
					(TableFilter::Partitioned(filters), true) => {
						assert!(filters.num_partitions() > 2, "{what}")
					}
					(TableFilter::None, false) => {}
					_ => panic!("{what}: the table has not the filter that was asked for"),
				}
				assert_eq!(table.meta.properties.id, TABLE_ID, "{what}");
				assert_eq!(table.meta.properties.num_entries, KEYS as u64, "{what}");

				for i in 0..KEYS {
					let (key, value) = table.get(&lookup(i)).unwrap().expect("the key is there");
					assert_eq!(key.user_key, key_of(i), "{what}");
					assert_eq!(value, value_of(i), "{what}");
				}
				// A key before the first, between two, and after the last.
				for absent in [&b"aaa"[..], &b"key0050x"[..], &b"zzz"[..]] {
					let key = InternalKey::new(
						absent.to_vec(),
						INTERNAL_KEY_SEQ_NUM_MAX,
						InternalKeyKind::Set,
					);
					assert!(table.get(&key).unwrap().is_none(), "{what}");
				}

				let all = scan(&table, None);
				assert_eq!(all.len(), KEYS, "{what}");
				for (i, (key, value)) in all.iter().enumerate() {
					assert_eq!((key, value), (&key_of(i), &value_of(i)), "{what}");
				}
				let range = (Bound::Included(lookup(10)), Bound::Excluded(lookup(20)));
				let inside: Vec<Vec<u8>> =
					scan(&table, Some(range)).into_iter().map(|(key, _)| key).collect();
				assert_eq!(inside, (10..20).map(key_of).collect::<Vec<_>>(), "{what}");

				// And backwards.
				let mut iter = table.iter(None).unwrap();
				iter.seek_last().unwrap();
				let mut backwards = Vec::new();
				while iter.valid() {
					backwards.push(iter.key().user_key().to_vec());
					iter.prev().unwrap();
				}
				assert_eq!(backwards, (0..KEYS).rev().map(key_of).collect::<Vec<_>>(), "{what}");
			}
		}
	}
}

#[tokio::test]
async fn the_asynchronous_path_reads_the_blocks_of_a_sealed_table() {
	for suite in SUITES {
		let ctx = context(suite);
		let opts = options(Some(&ctx), CompressionType::SnappyCompression, true);
		let file = write_table(&opts, TABLE_ID, &many_points(KEYS));
		let table = open_table(&opts, &file, TABLE_ID).unwrap();

		for i in (0..KEYS).step_by(7) {
			let (_, value) = table.get_async(&lookup(i)).await.unwrap().expect("the key is there");
			assert_eq!(value, value_of(i), "{suite:?}");
		}
	}
}

#[test]
fn a_sealed_table_of_range_deletions_alone_is_read_back() {
	for suite in SUITES {
		let ctx = context(suite);
		let opts = options(Some(&ctx), CompressionType::None, true);
		let input = Input {
			points: Vec::new(),
			range_deletions: vec![(b"a".to_vec(), b"z".to_vec(), 7)],
		};
		let file = write_table(&opts, TABLE_ID, &input);
		let table = open_table(&opts, &file, TABLE_ID).unwrap();

		assert!(table.holds_only_range_deletions(), "{suite:?}");
		assert_eq!(table.meta.range_deletions, input.range_deletions, "{suite:?}");
		assert!(table.get(&lookup(0)).unwrap().is_none());
	}
}

// ========== A table is opened for the database that it belongs to ==========

fn footer_range(file: &[u8]) -> (u64, usize) {
	((file.len() - TABLE_FULL_FOOTER_LENGTH) as u64, TABLE_FULL_FOOTER_LENGTH)
}

const HEADER_RANGE: (u64, usize) = (0, HEADER_LEN);

#[test]
fn a_table_is_refused_by_a_database_of_the_other_kind_after_reading_only_its_footer() {
	let ctx = context(CipherSuite::Aes256Gcm);
	let encrypted = options(Some(&ctx), CompressionType::None, true);
	let plain = options(None, CompressionType::None, true);
	let sealed_file = write_table(&encrypted, TABLE_ID, &many_points(KEYS));
	let plain_file = write_table(&plain, TABLE_ID, &many_points(KEYS));

	// A sealed table in a database that has no key.
	let file = RecordingFile::new(sealed_file.clone());
	let size = sealed_file.len() as u64;
	let err = encryption_error(Table::new(TABLE_ID, Arc::clone(&plain), file.shared(), size));
	assert_eq!(err, EncryptionError::NotConfigured);
	assert_eq!(file.reads(), vec![footer_range(&sealed_file)]);

	// A plain table in an encrypted database is not read as a plain table, whatever its checksums
	// say: it is refused, and nothing of it is parsed.
	let file = RecordingFile::new(plain_file.clone());
	let size = plain_file.len() as u64;
	let err = encryption_error(Table::new(TABLE_ID, Arc::clone(&encrypted), file.shared(), size));
	assert_eq!(err, EncryptionError::PlaintextInEncryptedDb);
	assert_eq!(file.reads(), vec![footer_range(&plain_file)]);

	// The two that are not refused: each database opens its own kind of table, with the footer
	// first and then, for the sealed one, the header.
	let file = RecordingFile::new(plain_file.clone());
	assert!(Table::new(TABLE_ID, Arc::clone(&plain), file.shared(), size).is_ok());
	assert_eq!(file.reads()[0], footer_range(&plain_file));
	let file = RecordingFile::new(sealed_file.clone());
	let size = sealed_file.len() as u64;
	assert!(Table::new(TABLE_ID, Arc::clone(&encrypted), file.shared(), size).is_ok());
	assert_eq!(file.reads()[..2], [footer_range(&sealed_file), HEADER_RANGE]);
}

/// The contexts that are wrong for a table that was written for `TABLE_ID` under key 1 of
/// `manager()`, and what each is refused with.
fn wrong_contexts(suite: CipherSuite) -> Vec<(&'static str, Arc<Options>, u64, EncryptionError)> {
	let with = |ctx: &Arc<EncryptionContext>| options(Some(ctx), CompressionType::None, true);

	// Another key behind the same key id.
	let other_root = context_for(Arc::new(SoftwareKeyManager::new([0x99; ROOT_KEY_LEN])), suite);
	// The same key, in another database.
	let other_db = Arc::new(EncryptionContext::new(manager(), DbId::from_bytes([0xD6; 16]), suite));
	// A manager that has never held key 1.
	let without_the_key = {
		let km = SoftwareKeyManager::new([0x42; ROOT_KEY_LEN]);
		km.add_key(2, [0x43; ROOT_KEY_LEN]).unwrap();
		km.set_active_key_id(2).unwrap();
		km.retire_key(1).unwrap();
		context_for(Arc::new(km), suite)
	};

	vec![
		("another key", with(&other_root), TABLE_ID, EncryptionError::WrongKey),
		("another database", with(&other_db), TABLE_ID, EncryptionError::DbIdMismatch),
		("a retired key", with(&without_the_key), TABLE_ID, EncryptionError::KeyRetired),
		("another table id", with(&context(suite)), TABLE_ID + 1, EncryptionError::FileIdMismatch),
	]
}

#[test]
fn a_sealed_table_is_refused_by_the_wrong_database_or_key_after_reading_its_footer_and_header() {
	for suite in SUITES {
		let ctx = context(suite);
		let opts = options(Some(&ctx), CompressionType::None, true);
		let file = write_table(&opts, TABLE_ID, &many_points(KEYS));
		let size = file.len() as u64;

		for (what, wrong, id, expected) in wrong_contexts(suite) {
			let recorded = RecordingFile::new(file.clone());
			let err = encryption_error(Table::new(id, wrong, recorded.shared(), size));
			assert_eq!(err, expected, "{suite:?}: {what}");
			assert_eq!(
				recorded.reads(),
				vec![footer_range(&file), HEADER_RANGE],
				"{suite:?}: {what}: nothing but the footer and the header was read"
			);
		}

		// The right context opens it, and so does one whose key was rotated since: files stay
		// readable under a key that is no longer the active one.
		assert!(open_table(&opts, &file, TABLE_ID).is_ok());
		let km = manager();
		let rotated = context_for(Arc::<SoftwareKeyManager>::clone(&km), suite);
		let rotated_opts = options(Some(&rotated), CompressionType::None, true);
		let file = write_table(&rotated_opts, TABLE_ID, &many_points(KEYS));
		km.add_key(2, [0x43; ROOT_KEY_LEN]).unwrap();
		km.set_active_key_id(2).unwrap();
		let table = open_table(&rotated_opts, &file, TABLE_ID).unwrap();
		assert_eq!(table.get(&lookup(3)).unwrap().unwrap().1, value_of(3));
	}
}

// ========== A damaged filter ==========

/// The blocks of a table with the kind of each, found by following the handles from the footer
/// through the reader of the engine, which opens `file` as table `id` of the database of `opts`.
fn walk(opts: &Arc<Options>, file: &[u8], id: u64) -> Vec<(BlockKind, BlockHandle)> {
	let size = file.len() as u64;
	let (source, footer) =
		BlockSource::open_table(Arc::new(file.to_vec()), size, id, opts.encryption_context())
			.unwrap();
	let comparator = || Arc::clone(&opts.internal_comparator);
	let entries = |kind: BlockKind, handle: &BlockHandle| -> Vec<(Vec<u8>, Vec<u8>)> {
		let block = source.read_block(kind, handle, comparator()).unwrap();
		let mut iter = block.iter().unwrap();
		iter.seek_to_first().unwrap();
		let mut found = Vec::new();
		while iter.is_valid() {
			found.push((iter.key_bytes().to_vec(), iter.value_bytes().to_vec()));
			iter.advance().unwrap();
		}
		found
	};

	let mut blocks = vec![
		(BlockKind::TopLevelIndex, footer.index.clone()),
		(BlockKind::MetaIndex, footer.meta_index.clone()),
	];
	for (_, partition) in entries(BlockKind::TopLevelIndex, &footer.index) {
		let (partition, _) = BlockHandle::decode(&partition).unwrap();
		for (_, data) in entries(BlockKind::IndexPartition, &partition) {
			blocks.push((BlockKind::Data, BlockHandle::decode(&data).unwrap().0));
		}
		blocks.push((BlockKind::IndexPartition, partition));
	}
	for (key, value) in entries(BlockKind::MetaIndex, &footer.meta_index) {
		if InternalKey::decode(&key).user_key.starts_with(b"partitionedfilter.") {
			let (filter_index, _) = BlockHandle::decode(&value).unwrap();
			for (_, partition) in entries(BlockKind::FilterIndex, &filter_index) {
				blocks
					.push((BlockKind::FilterPartition, BlockHandle::decode(&partition).unwrap().0));
			}
			blocks.push((BlockKind::FilterIndex, filter_index));
		}
	}
	blocks.sort_by_key(|(_, handle)| handle.offset);
	blocks
}

/// `file` with a bit flipped `at` bytes into the block at `handle`. With `fix_trailer` the trailer
/// of the block is made again to say that the block is as it is, as someone who has no key can,
/// and the damage is then for the seal to find and not for the checksum.
fn flip(
	file: &[u8],
	handle: &BlockHandle,
	at: usize,
	layout: TrailerLayout,
	fix_trailer: bool,
) -> Vec<u8> {
	let mut damaged = file.to_vec();
	damaged[handle.offset + at] ^= 0x01;
	if fix_trailer {
		let end = handle.offset + handle.size;
		let compression = damaged[end];
		let trailer = Trailer::encode(layout, &damaged[handle.offset..end], compression).unwrap();
		damaged[end..end + layout.len()].copy_from_slice(trailer.as_slice());
	}
	damaged
}

fn first_of(blocks: &[(BlockKind, BlockHandle)], kind: BlockKind) -> BlockHandle {
	blocks.iter().find(|(k, _)| *k == kind).expect("the table has a block of the kind").1.clone()
}

#[test]
fn a_filter_partition_that_cannot_be_read_is_skipped_in_a_plain_table_as_it_always_was() {
	let opts = options(None, CompressionType::None, true);
	let file = write_table(&opts, TABLE_ID, &many_points(KEYS));
	let partition = first_of(&walk(&opts, &file, TABLE_ID), BlockKind::FilterPartition);
	let damaged = flip(&file, &partition, 0, TrailerLayout::Plain, false);

	let table = open_table(&opts, &damaged, TABLE_ID).unwrap();
	assert_eq!(table.filter_reads_degraded(), 0);
	// The filter is only an optimisation: the lookup is answered without it, and the number of
	// those that were is kept.
	assert_eq!(table.get(&lookup(0)).unwrap().unwrap().1, value_of(0));
	assert!(table.filter_reads_degraded() > 0);
}

#[tokio::test]
async fn a_filter_partition_that_is_damaged_but_not_forged_is_skipped_in_a_sealed_table_too() {
	// Without the key nobody can make a partition that opens, and a partition that fails its
	// checksum is what bit rot looks like: the lookup does not need the filter.
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(Some(&ctx), CompressionType::None, true);
	let file = write_table(&opts, TABLE_ID, &many_points(KEYS));
	let partition = first_of(&walk(&opts, &file, TABLE_ID), BlockKind::FilterPartition);
	let damaged = flip(&file, &partition, 0, TrailerLayout::Sealed, false);

	let table = open_table(&opts, &damaged, TABLE_ID).unwrap();
	assert_eq!(table.get(&lookup(0)).unwrap().unwrap().1, value_of(0));
	assert_eq!(table.get_async(&lookup(1)).await.unwrap().unwrap().1, value_of(1));
	assert!(table.filter_reads_degraded() > 0);
}

#[tokio::test]
async fn a_filter_partition_that_fails_authentication_is_an_error_and_not_a_maybe() {
	for suite in SUITES {
		let ctx = context(suite);
		let opts = options(Some(&ctx), CompressionType::None, true);
		let file = write_table(&opts, TABLE_ID, &many_points(KEYS));
		let partition = first_of(&walk(&opts, &file, TABLE_ID), BlockKind::FilterPartition);
		// The checksum is right: only the seal can tell.
		let damaged = flip(&file, &partition, 0, TrailerLayout::Sealed, true);

		let table = open_table(&opts, &damaged, TABLE_ID).unwrap();
		let err = table.get(&lookup(0)).unwrap_err();
		assert!(
			matches!(err, Error::Encryption(EncryptionError::AuthenticationFailed)),
			"{suite:?}: {err:?}"
		);
		let err = table.get_async(&lookup(0)).await.unwrap_err();
		assert!(
			matches!(err, Error::Encryption(EncryptionError::AuthenticationFailed)),
			"{suite:?}: {err:?}"
		);
		assert_eq!(table.filter_reads_degraded(), 0, "{suite:?}: nothing was skipped");
	}
}
