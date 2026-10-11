//! A sealed WAL segment: what its header says, how its batches are sealed and framed, that every
//! one of them reads back, and that a segment of one kind is never taken for the other.
//!
//! The helpers at the top are shared by the other files of the sealed WAL tests. The data is kept
//! small, since AEAD runs at a few MB/s in the debug profile.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use crate::batch::Batch;
use crate::error::EncryptionError;
use crate::tde::{
	BlockContext,
	BlockKind,
	CipherSuite,
	DbId,
	EncryptionContext,
	FileKind,
	SoftwareKeyManager,
	HEADER_LEN,
	TAG_LEN,
};
use crate::wal::manager::Wal;
use crate::wal::reader::Reader;
use crate::wal::{
	calculate_crc32,
	sealed,
	segment_name,
	CompressionType,
	Error as WalError,
	Options as WalOptions,
	RecordType,
	WalContext,
	BLOCK_SIZE,
	HEADER_SIZE,
};

// ---------------------------------------------------------------------------
// helpers, shared with the other sealed WAL tests
// ---------------------------------------------------------------------------

pub(crate) const SUITES: [CipherSuite; 2] =
	[CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

pub(crate) const COMPRESSIONS: [CompressionType; 2] = [CompressionType::None, CompressionType::Lz4];

pub(crate) const ROOT: [u8; 32] = [0x11; 32];
pub(crate) const DB: [u8; 16] = [0xD0; 16];

/// Length of the header record of a sealed segment, framing included: where its first batch is.
pub(crate) const HEADER_RECORD_LEN: usize = HEADER_SIZE + sealed::HEADER_PAYLOAD_LEN;

pub(crate) fn context_of(
	root: [u8; 32],
	db: [u8; 16],
	suite: CipherSuite,
) -> Arc<EncryptionContext> {
	Arc::new(EncryptionContext::new(
		Arc::new(SoftwareKeyManager::new(root)),
		DbId::from_bytes(db),
		suite,
	))
}

/// The context of the database that the tests write to.
pub(crate) fn context(suite: CipherSuite) -> Arc<EncryptionContext> {
	context_of(ROOT, DB, suite)
}

pub(crate) fn sealed_context(enc: &Arc<EncryptionContext>) -> WalContext {
	WalContext::sealed(Arc::clone(enc))
}

/// The options of a sealed WAL whose new segments are compressed as given.
pub(crate) fn sealed_options(
	enc: &Arc<EncryptionContext>,
	compression: CompressionType,
) -> WalOptions {
	WalOptions {
		context: sealed_context(enc),
		compression_type: compression,
		..WalOptions::default()
	}
}

pub(crate) fn segment_path(dir: &Path, id: u64) -> PathBuf {
	dir.join(segment_name(id, "wal"))
}

/// A record of `len` bytes that depends on `seed`, and that LZ4 can shrink.
pub(crate) fn record(seed: usize, len: usize) -> Vec<u8> {
	(0..len).map(|i| b'a' + ((seed + i / 7) % 23) as u8).collect()
}

/// A batch with `entries` entries, encoded, that holds the marker `tag` in every key and value.
pub(crate) fn encoded_batch(seq: u64, tag: &str, entries: usize, value_len: usize) -> Vec<u8> {
	let mut batch = Batch::new(seq);
	for i in 0..entries {
		let key = format!("{tag}-key-{i:04}").into_bytes();
		let mut value = format!("{tag}-value-{i:04}-").into_bytes();
		value.resize(value_len.max(value.len()), b'.');
		batch.set(key, value, 0).unwrap();
	}
	batch.encode().unwrap()
}

/// Every record of a segment in order, or the error that stopped the read: the clean end of the
/// segment is the end of the list.
pub(crate) fn read_records(
	context: &WalContext,
	path: &Path,
	id: u64,
) -> Result<Vec<Vec<u8>>, WalError> {
	let file = fs::File::open(path).unwrap();
	let mut reader = Reader::with_context(file, None, id, context);
	let mut records = Vec::new();
	loop {
		match reader.read() {
			Ok((data, _)) => records.push(data.to_vec()),
			Err(WalError::IO(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
				return Ok(records)
			}
			Err(e) => return Err(e),
		}
	}
}

/// The physical records of a file, found without the reader: where each begins, its type byte and
/// the length of its payload. A record of type zero ends its block.
pub(crate) fn physical_records(bytes: &[u8]) -> Vec<(usize, u8, usize)> {
	let mut found = Vec::new();
	let mut block = 0;
	while block * BLOCK_SIZE < bytes.len() {
		let end = ((block + 1) * BLOCK_SIZE).min(bytes.len());
		let mut at = block * BLOCK_SIZE;
		while at + HEADER_SIZE <= end {
			let length = usize::from(u16::from_be_bytes([bytes[at + 4], bytes[at + 5]]));
			let kind = bytes[at + 6];
			if kind == 0 {
				break;
			}
			found.push((at, kind, length));
			at += HEADER_SIZE + length;
		}
		block += 1;
	}
	found
}

/// Sets the checksum of the physical record at `at` to what its type and payload give, as someone
/// who changed the record without the key can.
pub(crate) fn fix_checksum(bytes: &mut [u8], at: usize) {
	let length = usize::from(u16::from_be_bytes([bytes[at + 4], bytes[at + 5]]));
	let crc =
		calculate_crc32(&[bytes[at + 6]], &bytes[at + HEADER_SIZE..at + HEADER_SIZE + length]);
	bytes[at..at + 4].copy_from_slice(&crc.to_be_bytes());
}

/// Every file under `dir`, by path relative to it, with its bytes.
pub(crate) fn snapshot(dir: &Path) -> BTreeMap<String, Vec<u8>> {
	fn visit(root: &Path, dir: &Path, files: &mut BTreeMap<String, Vec<u8>>) {
		for entry in fs::read_dir(dir).unwrap() {
			let path = entry.unwrap().path();
			if path.is_dir() {
				visit(root, &path, files);
			} else {
				let name = path.strip_prefix(root).unwrap().to_string_lossy().into_owned();
				files.insert(name, fs::read(&path).unwrap());
			}
		}
	}
	let mut files = BTreeMap::new();
	visit(dir, dir, &mut files);
	files
}

/// The names of the files of `dir`.
pub(crate) fn names(dir: &Path) -> Vec<String> {
	snapshot(dir).into_keys().collect()
}

/// Writes `records` to a new sealed WAL in `dir`, one `append` each, and closes it. Returns the
/// segment that they went to.
pub(crate) fn write_segment(
	dir: &Path,
	enc: &Arc<EncryptionContext>,
	compression: CompressionType,
	records: &[Vec<u8>],
) -> u64 {
	let mut wal = Wal::open(dir, sealed_options(enc, compression)).unwrap();
	for record in records {
		wal.append(record).unwrap();
	}
	let segment = wal.get_active_log_number();
	wal.close().unwrap();
	segment
}

// ---------------------------------------------------------------------------
// the header of a segment
// ---------------------------------------------------------------------------

#[test]
fn the_header_of_a_segment_names_the_file_and_what_the_segment_says_about_itself() {
	for suite in SUITES {
		for compression in COMPRESSIONS {
			let enc = context(suite);
			let (payload, writer) = sealed::new_header(&enc, 7, compression).unwrap();

			// 92 bytes of header and the two bytes that its tag covers.
			assert_eq!(payload.len(), 94);
			assert_eq!(payload.len(), HEADER_LEN + 2);
			assert_eq!(&payload[..4], b"SKTD", "{suite:?}");
			assert_eq!(payload[5], suite.id());
			assert_eq!(payload[6], FileKind::Wal.id());
			assert_eq!(&payload[16..32], &DB);
			assert_eq!(&payload[32..40], &7u64.to_be_bytes());
			assert_eq!(payload[HEADER_LEN], 1, "the format of the segment");
			assert_eq!(payload[HEADER_LEN + 1], compression as u8, "its compression");

			let (reader, found) = sealed::open_header(&enc, 7, &payload).unwrap();
			assert_eq!(found, compression);
			assert!(writer.can_seal());
			assert!(!reader.can_seal());
			assert_eq!(reader.identity(), writer.identity());
		}
	}
}

#[test]
fn every_new_header_has_a_salt_of_its_own_whatever_the_segment_number() {
	let enc = context(CipherSuite::Aes256Gcm);
	let (first, _) = sealed::new_header(&enc, 3, CompressionType::None).unwrap();
	let (again, _) = sealed::new_header(&enc, 3, CompressionType::None).unwrap();
	assert_eq!(first[..40], again[..40], "the same identity");
	assert_ne!(first[40..72], again[40..72], "a new salt");
	assert_ne!(first[72..88], again[72..88], "so a new tag");
}

/// What opening the header `payload` of the segment `id` fails with, if it does.
fn open_header_error(enc: &EncryptionContext, id: u64, payload: &[u8]) -> Option<EncryptionError> {
	sealed::open_header(enc, id, payload).err()
}

/// `payload` with the checksum of its encryption header made again.
fn with_header_checksum(
	mut payload: [u8; sealed::HEADER_PAYLOAD_LEN],
) -> [u8; sealed::HEADER_PAYLOAD_LEN] {
	let crc = crc32fast::hash(&payload[..HEADER_LEN - 4]);
	payload[HEADER_LEN - 4..HEADER_LEN].copy_from_slice(&crc.to_be_bytes());
	payload
}

#[test]
fn a_header_that_does_not_fit_is_refused_and_the_error_says_why() {
	for suite in SUITES {
		let enc = context(suite);
		let (payload, _) = sealed::new_header(&enc, 7, CompressionType::None).unwrap();
		let open = open_header_error;

		// What is true of the file and not of the key.
		let other_root = context_of([0x99; 32], DB, suite);
		assert_eq!(open(&other_root, 7, &payload), Some(EncryptionError::WrongKey));
		let other_db = context_of(ROOT, [0xD1; 16], suite);
		assert_eq!(open(&other_db, 7, &payload), Some(EncryptionError::DbIdMismatch));
		assert_eq!(open(&enc, 8, &payload), Some(EncryptionError::FileIdMismatch));

		// A byte that changed with the checksum of the header made again: nothing but the tag can
		// tell, and it says that the key is not the one that made the file.
		for at in [40, 63, HEADER_LEN] {
			let mut changed = payload;
			changed[at] ^= 0x01;
			let changed = with_header_checksum(changed);
			assert_eq!(open(&enc, 7, &changed), Some(EncryptionError::WrongKey), "byte {at}");
		}
		// The two bytes after the header are covered by its tag, and carry no checksum.
		let mut changed = payload;
		changed[HEADER_LEN + 1] ^= 0x01;
		assert_eq!(open(&enc, 7, &changed), Some(EncryptionError::WrongKey));

		// The same bytes without the checksum made again are damage.
		let mut damaged = payload;
		damaged[40] ^= 0x01;
		assert_eq!(open(&enc, 7, &damaged), Some(EncryptionError::MalformedHeader));
		let mut no_magic = payload;
		no_magic[0] ^= 0x01;
		assert_eq!(open(&enc, 7, &no_magic), Some(EncryptionError::MalformedHeader));
		assert_eq!(open(&enc, 7, &payload[..50]), Some(EncryptionError::MalformedHeader));
		assert_eq!(open(&enc, 7, &[]), Some(EncryptionError::MalformedHeader));

		// A key id that the manager has never held.
		let mut unknown = payload;
		unknown[8..12].copy_from_slice(&9u32.to_be_bytes());
		let unknown = with_header_checksum(unknown);
		assert_eq!(open(&enc, 7, &unknown), Some(EncryptionError::UnknownKeyId));
	}
}

#[test]
fn a_header_of_a_key_that_was_retired_is_refused_as_such() {
	let manager = Arc::new(SoftwareKeyManager::new(ROOT));
	let enc = Arc::new(EncryptionContext::new(
		Arc::clone(&manager) as Arc<dyn crate::tde::KeyManager>,
		DbId::from_bytes(DB),
		CipherSuite::Aes256Gcm,
	));
	let (payload, _) = sealed::new_header(&enc, 4, CompressionType::None).unwrap();
	manager.add_key(2, [0x22; 32]).unwrap();
	manager.set_active_key_id(2).unwrap();
	// Still readable: the key is only no longer the one that new files use.
	sealed::open_header(&enc, 4, &payload).unwrap();
	manager.retire_key(1).unwrap();
	assert_eq!(sealed::open_header(&enc, 4, &payload).err(), Some(EncryptionError::KeyRetired));
}

#[test]
fn what_the_header_says_is_believed_only_after_it_authenticated() {
	let enc = context(CipherSuite::XChaCha20Poly1305);
	// Headers that are authentic, made under the right key, and that say something this build
	// does not know: a later format, a compression it has no name for, more than it expects.
	let unknown: [&[u8]; 5] = [&[2, 0], &[1, 5], &[1, 0, 0], &[1], &[]];
	for covered in unknown {
		let (header, _) = enc.create_file(FileKind::Wal, 7, covered).unwrap();
		let mut payload = header.encode().to_vec();
		payload.extend_from_slice(covered);
		assert_eq!(
			sealed::open_header(&enc, 7, &payload).err(),
			Some(EncryptionError::UnsupportedVersion),
			"{covered:?}"
		);
	}
	// A file that is not a WAL segment is not one, whoever made it.
	let (header, _) = enc.create_file(FileKind::Vlog, 7, &[1, 0]).unwrap();
	let mut payload = header.encode().to_vec();
	payload.extend_from_slice(&[1, 0]);
	assert_eq!(sealed::open_header(&enc, 7, &payload).err(), Some(EncryptionError::FileIdMismatch));
}

// ---------------------------------------------------------------------------
// what a segment looks like on disk
// ---------------------------------------------------------------------------

#[test]
fn a_sealed_segment_is_a_header_record_and_one_sealed_record_per_batch() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let enc = context(suite);
		let first = record(1, 40);
		let second = record(2, 300);
		let segment = write_segment(
			dir.path(),
			&enc,
			CompressionType::None,
			&[first.clone(), second.clone()],
		);
		assert_eq!(segment, 0);

		let bytes = fs::read(segment_path(dir.path(), segment)).unwrap();
		let expected_len = HEADER_RECORD_LEN
			+ (HEADER_SIZE + first.len() + TAG_LEN)
			+ (HEADER_SIZE + second.len() + TAG_LEN);
		assert_eq!(bytes.len(), expected_len, "{suite:?}");

		let records = physical_records(&bytes);
		assert_eq!(
			records,
			vec![
				(0, RecordType::SegmentHeader as u8, sealed::HEADER_PAYLOAD_LEN),
				(HEADER_RECORD_LEN, RecordType::Full as u8, first.len() + TAG_LEN),
				(
					HEADER_RECORD_LEN + HEADER_SIZE + first.len() + TAG_LEN,
					RecordType::Full as u8,
					second.len() + TAG_LEN
				),
			]
		);

		// The header record: the checksum over its type and payload, a length of 94, type 10.
		assert_eq!(bytes[4..7], [0x00, 0x5E, 0x0A]);
		let header = &bytes[HEADER_SIZE..HEADER_RECORD_LEN];
		assert_eq!(bytes[..4], crc32fast::hash(&[&[0x0A][..], header].concat()).to_be_bytes());
		assert_eq!(&header[..4], b"SKTD");

		// The batches are sealed: the checksum is of the sealed bytes, which are not the batch.
		for (n, (at, _, length)) in records.iter().enumerate().skip(1) {
			let sealed_bytes = &bytes[at + HEADER_SIZE..at + HEADER_SIZE + length];
			let crc = crc32fast::hash(&[&[RecordType::Full as u8][..], sealed_bytes].concat());
			assert_eq!(bytes[*at..*at + 4], crc.to_be_bytes(), "record {n}");
			let plain = if n == 1 {
				&first
			} else {
				&second
			};
			assert_ne!(&sealed_bytes[..plain.len()], &plain[..], "record {n} is sealed");
		}
	}
}

#[test]
fn each_batch_is_sealed_at_its_index_counted_from_one_with_the_segment_and_the_database_bound() {
	for suite in SUITES {
		for compression in COMPRESSIONS {
			let dir = TempDir::new().unwrap();
			let enc = context(suite);
			// Records that LZ4 shrinks, so that what is sealed is the compressed form.
			let batches: Vec<Vec<u8>> = (0..4).map(|i| record(i, 200)).collect();
			let segment = write_segment(dir.path(), &enc, compression, &batches);
			let bytes = fs::read(segment_path(dir.path(), segment)).unwrap();
			let records = physical_records(&bytes);
			assert_eq!(records.len(), 1 + batches.len());

			// An independent reader of the file: the header opens the cipher, and each batch opens
			// at the position that is its index, and at no other.
			let header = &bytes[HEADER_SIZE..HEADER_RECORD_LEN];
			let cipher = enc
				.open_file_from_bytes(
					&header[..HEADER_LEN],
					FileKind::Wal,
					segment,
					&header[HEADER_LEN..],
				)
				.unwrap();
			for (n, (at, _, length)) in records.iter().enumerate().skip(1) {
				let sealed_bytes = bytes[at + HEADER_SIZE..at + HEADER_SIZE + length].to_vec();
				let at_index =
					|index: u64| BlockContext::new(BlockKind::WalBatch, index, compression as u8);

				for wrong in [n as u64 - 1, n as u64 + 1, 0] {
					let mut copy = sealed_bytes.clone();
					assert_eq!(
						cipher.open_suffix(&at_index(wrong), &mut copy).err(),
						Some(EncryptionError::AuthenticationFailed),
						"{suite:?} {compression:?}: batch {n} opened at {wrong}"
					);
				}
				// With the compression byte of the other setting it does not open either.
				let other = BlockContext::new(BlockKind::WalBatch, n as u64, 1 - compression as u8);
				let mut copy = sealed_bytes.clone();
				assert!(cipher.open_suffix(&other, &mut copy).is_err());

				let mut copy = sealed_bytes.clone();
				let plain = cipher.open_suffix(&at_index(n as u64), &mut copy).unwrap().to_vec();
				let batch = match compression {
					CompressionType::None => plain,
					CompressionType::Lz4 => lz4_flex::decompress_size_prepended(&plain).unwrap(),
				};
				assert_eq!(batch, batches[n - 1], "{suite:?} {compression:?}: batch {n}");
			}
		}
	}
}

#[test]
fn a_plaintext_segment_is_framed_as_it_always_was() {
	let dir = TempDir::new().unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	wal.append(b"abc").unwrap();
	wal.append(b"hello world").unwrap();
	wal.close().unwrap();

	// The checksums are the CRC-32 of the type byte and the payload, computed by zlib.
	let expected = ["539d20a9000301616263", "86bd2a69000b0168656c6c6f20776f726c64"].concat();
	let bytes = fs::read(segment_path(dir.path(), 0)).unwrap();
	let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
	assert_eq!(hex, expected);
}

// ---------------------------------------------------------------------------
// writing and reading back
// ---------------------------------------------------------------------------

#[test]
fn records_of_every_size_read_back_in_both_suites_with_and_without_compression() {
	// One block holds 32761 bytes of payload after a record header: these sizes are inside a
	// block, on both sides of its end, and over several.
	let sizes = [1usize, 16, 100, 5000, 32_700, 32_761, 32_800, 70_000];
	for suite in SUITES {
		for compression in COMPRESSIONS {
			let dir = TempDir::new().unwrap();
			let enc = context(suite);
			let records: Vec<Vec<u8>> =
				sizes.iter().enumerate().map(|(i, size)| record(i, *size)).collect();
			let segment = write_segment(dir.path(), &enc, compression, &records);

			let read =
				read_records(&sealed_context(&enc), &segment_path(dir.path(), segment), segment)
					.unwrap();
			assert_eq!(read, records, "{suite:?} {compression:?}");
		}
	}
}

/// Appends the records of `sizes` as one group, and notes what was appended in `expected`.
fn append_group(wal: &mut Wal, expected: &mut Vec<Vec<u8>>, sizes: &[usize]) -> u64 {
	let mut buf = Vec::new();
	let mut ends = Vec::new();
	let first = expected.len();
	for (i, size) in sizes.iter().enumerate() {
		buf.extend_from_slice(&record(first + i, *size));
		ends.push(buf.len());
	}
	let segment = wal.append_group(&buf, &ends).unwrap();
	for (i, size) in sizes.iter().enumerate() {
		expected.push(record(first + i, *size));
	}
	segment
}

#[test]
fn a_group_is_sealed_batch_by_batch_at_consecutive_indices_and_written_together() {
	for suite in SUITES {
		for compression in COMPRESSIONS {
			let dir = TempDir::new().unwrap();
			let enc = context(suite);
			let mut wal = Wal::open(dir.path(), sealed_options(&enc, compression)).unwrap();
			// A group, then a record of its own, then another group: the index goes on across all.
			let mut expected = Vec::new();
			assert_eq!(
				append_group(&mut wal, &mut expected, &[10, 20, 3000, 40, 50, 60, 70, 80]),
				0
			);
			assert_eq!(wal.next_index(), Some(9));
			let single = record(99, 123);
			wal.append(&single).unwrap();
			expected.push(single);
			assert_eq!(append_group(&mut wal, &mut expected, &[1, 2, 3]), 0);
			assert_eq!(wal.next_index(), Some(13));
			// The position the cipher would still seal is the same number: nothing was sealed
			// twice.
			assert_eq!(wal.cipher_next_position(), Some(13));
			wal.close().unwrap();

			let read =
				read_records(&sealed_context(&enc), &segment_path(dir.path(), 0), 0).unwrap();
			assert_eq!(read, expected, "{suite:?} {compression:?}");
		}
	}
}

#[test]
fn a_rotation_starts_a_segment_with_a_header_and_a_salt_of_its_own() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let enc = context(suite);
		let mut wal = Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();
		wal.append(b"in the first").unwrap();
		assert_eq!(wal.rotate().unwrap(), 1);
		// Numbering starts over in the new segment, under another key.
		assert_eq!(wal.next_index(), Some(1));
		wal.append(b"in the second").unwrap();
		wal.append(b"also in the second").unwrap();
		assert_eq!(wal.rotate().unwrap(), 2);
		wal.append(b"in the third").unwrap();
		wal.close().unwrap();

		let context = sealed_context(&enc);
		let expected: [(u64, &[&[u8]]); 3] = [
			(0, &[b"in the first"]),
			(1, &[b"in the second", b"also in the second"]),
			(2, &[b"in the third"]),
		];
		let mut salts = Vec::new();
		for (id, records) in expected {
			let path = segment_path(dir.path(), id);
			let read = read_records(&context, &path, id).unwrap();
			assert_eq!(read, records.iter().map(|r| r.to_vec()).collect::<Vec<_>>());
			let bytes = fs::read(&path).unwrap();
			assert_eq!(bytes[6], RecordType::SegmentHeader as u8);
			salts.push(bytes[HEADER_SIZE + 40..HEADER_SIZE + 72].to_vec());
		}
		salts.sort();
		salts.dedup();
		assert_eq!(salts.len(), 3, "{suite:?}: no two segments share a salt");
	}
}

#[test]
fn a_batch_that_is_recovered_is_the_batch_that_was_written() {
	let dir = TempDir::new().unwrap();
	let enc = context(CipherSuite::Aes256Gcm);
	let first = encoded_batch(100, "alpha", 3, 40);
	let second = encoded_batch(200, "beta", 2, 4000);
	write_segment(dir.path(), &enc, CompressionType::Lz4, &[first.clone(), second.clone()]);

	let context = sealed_context(&enc);
	let (max_seq, batches) = crate::wal::parallel_recovery::decode_segment_batches_with(
		&context,
		&segment_path(dir.path(), 0),
		0,
	)
	.unwrap();
	assert_eq!(max_seq, 201);
	assert_eq!(batches.len(), 2);
	assert_eq!(batches[0].encode().unwrap(), first);
	assert_eq!(batches[1].encode().unwrap(), second);
}

// ---------------------------------------------------------------------------
// when the segments are made
// ---------------------------------------------------------------------------

#[test]
fn opening_a_sealed_wal_creates_nothing_and_reserves_the_next_number() {
	let enc = context(CipherSuite::Aes256Gcm);
	let options = || sealed_options(&enc, CompressionType::None);

	// Nothing on disk: the number that is asked for.
	let dir = TempDir::new().unwrap();
	let wal = Wal::open_with_min_log_number(dir.path(), 5, options()).unwrap();
	assert_eq!(wal.get_active_log_number(), 5);
	assert!(!wal.has_segment());
	drop(wal);
	assert!(names(dir.path()).is_empty(), "nothing was created, and closing creates nothing");

	// Segments on disk: the one after the highest, never one of them, and never before the number
	// that is asked for.
	write_segment(dir.path(), &enc, CompressionType::None, &[record(0, 10)]);
	let before = snapshot(dir.path());
	assert_eq!(before.len(), 1);
	for (min, expected) in [(0, 1), (1, 1), (4, 4)] {
		let wal = Wal::open_with_min_log_number(dir.path(), min, options()).unwrap();
		assert_eq!(wal.get_active_log_number(), expected, "minimum {min}");
		assert!(!wal.has_segment());
	}
	assert_eq!(snapshot(dir.path()), before, "opening changed nothing");
}

#[test]
fn the_first_append_or_the_first_rotation_makes_the_segment_that_was_reserved() {
	let enc = context(CipherSuite::XChaCha20Poly1305);
	let options = || sealed_options(&enc, CompressionType::None);

	let by_append = TempDir::new().unwrap();
	let mut wal = Wal::open_with_min_log_number(by_append.path(), 3, options()).unwrap();
	assert!(names(by_append.path()).is_empty());
	wal.append(b"first").unwrap();
	assert!(wal.has_segment());
	assert_eq!(wal.get_active_log_number(), 3);
	assert_eq!(names(by_append.path()), [segment_name(3, "wal")]);
	wal.close().unwrap();

	let by_rotation = TempDir::new().unwrap();
	let mut wal = Wal::open_with_min_log_number(by_rotation.path(), 3, options()).unwrap();
	assert_eq!(wal.rotate().unwrap(), 3, "the number that was reserved, so there is no gap");
	assert_eq!(wal.get_active_log_number(), 3);
	assert_eq!(names(by_rotation.path()), [segment_name(3, "wal")]);
	// A header and nothing else, durable.
	assert_eq!(
		fs::metadata(segment_path(by_rotation.path(), 3)).unwrap().len() as usize,
		HEADER_RECORD_LEN
	);
	assert_eq!(wal.rotate().unwrap(), 4);
	wal.close().unwrap();
}

#[test]
fn a_restart_never_appends_to_a_sealed_segment_it_starts_one_of_its_own() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let enc = context(suite);
		let first =
			write_segment(dir.path(), &enc, CompressionType::None, &[record(1, 50), record(2, 60)]);
		let before = fs::read(segment_path(dir.path(), first)).unwrap();

		let mut wal = Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();
		assert_eq!(wal.get_active_log_number(), first + 1);
		wal.append(&record(3, 70)).unwrap();
		wal.close().unwrap();

		assert_eq!(fs::read(segment_path(dir.path(), first)).unwrap(), before);
		let second = fs::read(segment_path(dir.path(), first + 1)).unwrap();
		assert_ne!(
			before[HEADER_SIZE + 40..HEADER_SIZE + 72],
			second[HEADER_SIZE + 40..HEADER_SIZE + 72]
		);
		// Numbering starts at one again, under the new salt.
		let read =
			read_records(&sealed_context(&enc), &segment_path(dir.path(), first + 1), first + 1)
				.unwrap();
		assert_eq!(read, vec![record(3, 70)]);
	}
}

#[test]
fn a_file_that_is_in_the_way_is_an_error_and_is_never_appended_to() {
	let dir = TempDir::new().unwrap();
	let enc = context(CipherSuite::Aes256Gcm);
	let mut wal = Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();
	// Something that appeared at the reserved number, as a segment of an earlier run would.
	let in_the_way = segment_path(dir.path(), 0);
	fs::write(&in_the_way, b"already here").unwrap();

	for _ in 0..2 {
		let error = wal.append(b"never written").unwrap_err();
		match error {
			WalError::IO(e) => assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists),
			other => panic!("expected an AlreadyExists error, got {other}"),
		}
		assert!(!wal.has_segment());
		assert_eq!(fs::read(&in_the_way).unwrap(), b"already here");
	}
	assert_eq!(names(dir.path()), [segment_name(0, "wal")]);
}

// ---------------------------------------------------------------------------
// a segment of one kind is not the other's
// ---------------------------------------------------------------------------

#[test]
fn a_plaintext_segment_is_refused_by_a_sealed_reader_whatever_it_starts_with() {
	let enc = context(CipherSuite::Aes256Gcm);
	for compression in COMPRESSIONS {
		let dir = TempDir::new().unwrap();
		let options = WalOptions::default().with_compression(compression);
		let mut wal = Wal::open(dir.path(), options).unwrap();
		wal.append(b"a plaintext record").unwrap();
		wal.close().unwrap();
		let before = snapshot(dir.path());

		// The first record is a batch, or the record that says how the batches are compressed.
		let first_type = fs::read(segment_path(dir.path(), 0)).unwrap()[6];
		assert_eq!(
			first_type,
			if compression == CompressionType::None {
				1
			} else {
				9
			}
		);

		let error =
			read_records(&sealed_context(&enc), &segment_path(dir.path(), 0), 0).unwrap_err();
		assert_eq!(error, WalError::Encryption(EncryptionError::PlaintextInEncryptedDb));
		assert_eq!(snapshot(dir.path()), before);
	}
}

#[test]
fn a_sealed_segment_is_refused_by_a_plaintext_reader_and_a_plaintext_wal_does_not_open_on_it() {
	let dir = TempDir::new().unwrap();
	let enc = context(CipherSuite::XChaCha20Poly1305);
	write_segment(dir.path(), &enc, CompressionType::None, &[record(1, 50)]);
	let before = snapshot(dir.path());

	let error =
		read_records(&WalContext::plaintext(), &segment_path(dir.path(), 0), 0).unwrap_err();
	assert_eq!(error, WalError::Encryption(EncryptionError::NotConfigured));

	// A WAL that is not sealed does not open the segment for appending, and does not change it.
	let opened = Wal::open_with_min_log_number(dir.path(), 0, WalOptions::default());
	assert_eq!(opened.err(), Some(WalError::Encryption(EncryptionError::NotConfigured)));
	assert_eq!(snapshot(dir.path()), before);
}

#[test]
fn a_segment_that_cannot_be_opened_with_the_key_at_hand_changes_nothing() {
	let dir = TempDir::new().unwrap();
	let enc = context(CipherSuite::Aes256Gcm);
	write_segment(dir.path(), &enc, CompressionType::None, &[record(1, 50), record(2, 60)]);
	let before = snapshot(dir.path());
	let path = segment_path(dir.path(), 0);

	let other_root = context_of([0x99; 32], DB, CipherSuite::Aes256Gcm);
	let other_db = context_of(ROOT, [0xD1; 16], CipherSuite::Aes256Gcm);
	for (wrong, expected) in
		[(other_root, EncryptionError::WrongKey), (other_db, EncryptionError::DbIdMismatch)]
	{
		let error = read_records(&sealed_context(&wrong), &path, 0).unwrap_err();
		assert_eq!(error, WalError::Encryption(expected));
		// A file of another number is not this segment, even with the right key.
		let renamed = segment_path(dir.path(), 1);
		fs::copy(&path, &renamed).unwrap();
		let error = read_records(&sealed_context(&enc), &renamed, 1).unwrap_err();
		assert_eq!(error, WalError::Encryption(EncryptionError::FileIdMismatch));
		fs::remove_file(&renamed).unwrap();
	}
	assert_eq!(snapshot(dir.path()), before);
}
