//! What the parsers of an encrypted database do with bytes that are not what a writer wrote.
//!
//! Every file of an encrypted database is read before it is trusted: the header of a file, the
//! container of the manifest, the header and the entries of a value-log file, the records of a
//! segment of the log, the footer and the blocks of a table. Each of them is given here a file that
//! a real writer made with a real cipher, and then that file cut at every length and changed in
//! many places, and what must hold is that nothing panics, nothing is allocated from a length that
//! no one authenticated, and nothing that is not what was written is accepted as it:
//!
//! - a header, a manifest, a value-log header and an entry fail on every cut and on every flipped
//!   bit, since each of them is checksummed or authenticated as a whole;
//! - a segment of the log is read as far as it is whole, so what comes back is the start of what
//!   was written or an error, and never another record;
//! - a table opens, and then every key reads back as it was or fails, or does not open.
//!
//! The changes are drawn from a generator with a seed that is in every message, so a failure is
//! repeated by running the test again. The data is small, since AEAD is slow in an unoptimised
//! build, and the larger files are changed in fewer places.

use std::fs;
use std::sync::Arc;

use tempfile::TempDir;

use super::wal_sealed_tests::{
	context as wal_context,
	read_records,
	record,
	sealed_context,
	segment_path,
	write_segment,
};
use crate::sstable::block::BlockHandle;
use crate::sstable::table::{Footer, Table, TableWriter, TABLE_FULL_FOOTER_LENGTH};
use crate::tde::{
	describe_manifest,
	open_manifest,
	seal_manifest,
	CipherSuite,
	CryptoHeader,
	DbId,
	EncryptionContext,
	FileKind,
	SoftwareKeyManager,
	HEADER_LEN,
	MANIFEST_MAGIC,
};
use crate::vlog::{sealed as vlog_sealed, ValuePointer};
use crate::wal::{CompressionType as WalCompression, BLOCK_SIZE};
use crate::{CompressionType, InternalKey, InternalKeyKind, Options, INTERNAL_KEY_SEQ_NUM_MAX};

const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

/// The seed of every generator here, which is said by every message that a case fails with.
const SEED: u64 = 0x5EA1_ED00_F0E5;

/// SplitMix64: a few lines, no dependency, and the same numbers on every platform.
struct Rng(u64);

impl Rng {
	fn new(salt: u64) -> Self {
		Self(SEED ^ salt)
	}

	fn next_u64(&mut self) -> u64 {
		self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
		let mut z = self.0;
		z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
		z ^ (z >> 31)
	}

	/// A number below `n`, which is not zero.
	fn below(&mut self, n: usize) -> usize {
		(self.next_u64() % n as u64) as usize
	}

	fn byte(&mut self) -> u8 {
		self.next_u64() as u8
	}

	fn bytes(&mut self, len: usize) -> Vec<u8> {
		(0..len).map(|_| self.byte()).collect()
	}
}

/// `bytes` with the bit `bit` of the byte `at` changed.
fn flipped(bytes: &[u8], at: usize, bit: usize) -> Vec<u8> {
	let mut changed = bytes.to_vec();
	changed[at] ^= 1 << bit;
	changed
}

/// `bytes` with `count` bits changed, each at a place of its own.
fn with_flips(rng: &mut Rng, bytes: &[u8], count: usize) -> Vec<u8> {
	let mut places: Vec<(usize, usize)> = Vec::new();
	while places.len() < count {
		let place = (rng.below(bytes.len()), rng.below(8));
		if !places.contains(&place) {
			places.push(place);
		}
	}
	let mut changed = bytes.to_vec();
	for (at, bit) in places {
		changed[at] ^= 1 << bit;
	}
	changed
}

/// `bytes` with up to `max` bytes at random places set to random values, which is damage that a
/// checksum does not see through a single bit.
fn with_splice(rng: &mut Rng, bytes: &[u8], max: usize) -> Vec<u8> {
	let mut changed = bytes.to_vec();
	for _ in 0..=rng.below(max) {
		let at = rng.below(changed.len());
		changed[at] = rng.byte();
	}
	changed
}

/// `bytes` with a span of up to `max` bytes set to zero, as a block that was never written is.
fn with_zeros(rng: &mut Rng, bytes: &[u8], max: usize) -> Vec<u8> {
	let mut changed = bytes.to_vec();
	let at = rng.below(changed.len());
	let end = (at + 1 + rng.below(max)).min(changed.len());
	changed[at..end].fill(0);
	changed
}

fn context(suite: CipherSuite) -> EncryptionContext {
	EncryptionContext::new(
		Arc::new(SoftwareKeyManager::new([0x6B; 32])),
		DbId::from_bytes([0xF1; 16]),
		suite,
	)
}

// ---------------------------------------------------------------------------
// the header of an encrypted file
// ---------------------------------------------------------------------------

#[test]
fn a_header_is_refused_at_every_cut_and_after_any_change_of_up_to_three_bits() {
	for suite in SUITES {
		let ctx = context(suite);
		let (header, _cipher) = ctx.create_file(FileKind::Sst, 5, b"covered").unwrap();
		let bytes = header.encode();
		assert_eq!(CryptoHeader::decode(&bytes).unwrap(), header, "{suite:?}");

		for len in 0..bytes.len() {
			assert!(CryptoHeader::decode(&bytes[..len]).is_err(), "{suite:?}: cut at {len}");
		}

		// Whatever follows a header is not part of it.
		let mut longer = bytes.to_vec();
		longer.extend_from_slice(&[0xEE; 33]);
		assert_eq!(CryptoHeader::decode(&longer).unwrap(), header, "{suite:?}");

		// The checksum of a header sees any change of up to three bits in a header this long.
		for at in 0..bytes.len() {
			for bit in 0..8 {
				let changed = flipped(&bytes, at, bit);
				assert!(
					CryptoHeader::decode(&changed).is_err(),
					"{suite:?}: bit {bit} of byte {at}, seed {SEED:#x}"
				);
			}
		}
		let mut rng = Rng::new(1);
		for case in 0..400 {
			let changed = with_flips(&mut rng, &bytes, 2 + case % 2);
			assert!(
				CryptoHeader::decode(&changed).is_err(),
				"{suite:?}: case {case}, seed {SEED:#x}"
			);
		}

		// Bytes that begin as a header does and are nothing else.
		for case in 0..400 {
			let mut garbage = rng.bytes(HEADER_LEN);
			garbage[..5].copy_from_slice(&bytes[..5]);
			assert!(
				CryptoHeader::decode(&garbage).is_err(),
				"{suite:?}: garbage {case}, seed {SEED:#x}"
			);
		}
	}
}

// ---------------------------------------------------------------------------
// the container of an encrypted manifest
// ---------------------------------------------------------------------------

#[test]
fn a_sealed_manifest_is_refused_at_every_cut_and_after_every_flipped_bit() {
	for suite in SUITES {
		let ctx = context(suite);
		let body = b"a few bytes that stand for the encoding of a manifest".to_vec();
		let file = seal_manifest(&ctx, &body).unwrap();
		assert_eq!(open_manifest(&ctx, &file).unwrap(), body, "{suite:?}");

		for len in 0..file.len() {
			assert!(open_manifest(&ctx, &file[..len]).is_err(), "{suite:?}: cut at {len}");
			// What is said without a key never reads past what is there.
			let _ = describe_manifest(&file[..len]);
		}
		let mut longer = file.clone();
		longer.push(0);
		assert!(open_manifest(&ctx, &longer).is_err(), "{suite:?}: a byte too many");

		for at in 0..file.len() {
			for bit in 0..8 {
				let changed = flipped(&file, at, bit);
				assert!(
					open_manifest(&ctx, &changed).is_err(),
					"{suite:?}: bit {bit} of byte {at}, seed {SEED:#x}"
				);
				let _ = describe_manifest(&changed);
			}
		}

		// Bytes that begin as a manifest does and are nothing else: the lengths that they claim are
		// not believed.
		let mut rng = Rng::new(2);
		for case in 0..300 {
			let mut garbage = MANIFEST_MAGIC.to_vec();
			let more = rng.below(300);
			garbage.extend_from_slice(&rng.bytes(more));
			assert!(
				open_manifest(&ctx, &garbage).is_err(),
				"{suite:?}: garbage {case}, seed {SEED:#x}"
			);
			let _ = describe_manifest(&garbage);
		}
	}
}

// ---------------------------------------------------------------------------
// the header and the entries of a sealed value-log file
// ---------------------------------------------------------------------------

const FILE_ID: u32 = 0x0102_0304;
const MAX_FILE_SIZE: u64 = 0x0A0B_0C0D_0E0F_1011;

#[test]
fn a_value_log_header_is_refused_at_every_cut_and_after_every_flipped_bit() {
	for suite in SUITES {
		let ctx = context(suite);
		let dir = TempDir::new().unwrap();
		let path = dir.path().join("00000000000000000001.vlog");
		let (_file, _cipher) = vlog_sealed::create(&path, FILE_ID, MAX_FILE_SIZE, 0, &ctx).unwrap();
		let head = fs::read(&path).unwrap();
		assert_eq!(head.len(), vlog_sealed::SEALED_HEADER_LEN);
		vlog_sealed::open_header(&ctx, FILE_ID, &head, true).unwrap();

		for len in 0..head.len() {
			let result = vlog_sealed::open_header(&ctx, FILE_ID, &head[..len], false);
			assert!(result.is_err(), "{suite:?}: cut at {len}");
		}
		for at in 0..head.len() {
			for bit in 0..8 {
				let result =
					vlog_sealed::open_header(&ctx, FILE_ID, &flipped(&head, at, bit), false);
				assert!(result.is_err(), "{suite:?}: bit {bit} of byte {at}, seed {SEED:#x}");
			}
		}
		// The same header for another file is not this file's.
		assert!(vlog_sealed::open_header(&ctx, FILE_ID + 1, &head, false).is_err(), "{suite:?}");
		// And the torn-file verdict reads what it is given, whatever that is.
		let mut rng = Rng::new(3);
		for case in 0..300 {
			let len = rng.below(2 * vlog_sealed::SEALED_HEADER_LEN);
			let garbage = rng.bytes(len);
			let _ = vlog_sealed::is_torn(&garbage);
			let _ = vlog_sealed::is_sealed_start(&garbage);
			let result = vlog_sealed::open_header(&ctx, FILE_ID, &garbage, false);
			assert!(result.is_err(), "{suite:?}: garbage {case}, seed {SEED:#x}");
		}
	}
}

/// An entry of a sealed value-log file: what is stored for it, its pointer and its value.
struct SealedEntry {
	pointer: ValuePointer,
	stored: Vec<u8>,
	value: Vec<u8>,
}

/// The entries of a sealed value-log file in `ctx`, and the cipher that opens them.
fn sealed_entries(ctx: &EncryptionContext) -> (crate::tde::FileCipher, Vec<SealedEntry>) {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("00000000000000000001.vlog");
	let (_file, writer) = vlog_sealed::create(&path, FILE_ID, MAX_FILE_SIZE, 0, ctx).unwrap();
	let head = fs::read(&path).unwrap();
	let reader = vlog_sealed::open_header(ctx, FILE_ID, &head, true).unwrap();

	let mut offset = head.len() as u64;
	let mut entries = Vec::new();
	let contents: [(&[u8], Vec<u8>); 4] = [
		(&b"k"[..], vec![7u8; 10]),
		(&b""[..], b"a value with no key".to_vec()),
		(&b"a key with no value"[..], Vec::new()),
		(&b"a key that is longer"[..], vec![0x5A; 200]),
	];
	for (key, value) in contents {
		let mut stored = vlog_sealed::frame(key, &value).unwrap();
		vlog_sealed::seal(&writer, offset, &mut stored).unwrap();
		let pointer = ValuePointer::sealed(
			FILE_ID,
			offset,
			key.len() as u32,
			value.len() as u32,
			crc32fast::hash(&stored),
		);
		offset += stored.len() as u64;
		entries.push(SealedEntry {
			pointer,
			stored,
			value,
		});
	}
	(reader, entries)
}

#[test]
fn a_sealed_entry_is_refused_at_every_cut_and_after_every_flipped_bit_and_every_lying_pointer() {
	for suite in SUITES {
		let ctx = context(suite);
		let (reader, entries) = sealed_entries(&ctx);
		for (n, entry) in entries.iter().enumerate() {
			let SealedEntry {
				pointer,
				stored,
				value,
			} = entry;
			let what = format!("{suite:?}, entry {n}");
			assert_eq!(
				&vlog_sealed::open_entry(&reader, pointer, stored.clone()).unwrap(),
				value,
				"{what}"
			);

			for len in 0..stored.len() {
				let result = vlog_sealed::open_entry(&reader, pointer, stored[..len].to_vec());
				assert!(result.is_err(), "{what}: cut at {len}");
			}
			let mut longer = stored.clone();
			longer.push(0);
			assert!(vlog_sealed::open_entry(&reader, pointer, longer).is_err(), "{what}");

			for at in 0..stored.len() {
				for bit in 0..8 {
					let changed = flipped(stored, at, bit);
					// As the checksum of the pointer sees it, and then with the checksum that
					// someone who changed the bytes can make again, which is the tag's to refuse.
					let result = vlog_sealed::open_entry(&reader, pointer, changed.clone());
					assert!(result.is_err(), "{what}: bit {bit} of byte {at}");
					let made_again = ValuePointer {
						checksum: crc32fast::hash(&changed),
						..pointer.clone()
					};
					let result = vlog_sealed::open_entry(&reader, &made_again, changed);
					assert!(result.is_err(), "{what}: bit {bit} of byte {at}, checksum made again");
				}
			}

			// A pointer that says something else about the same bytes: another place, the
			// boundary between key and value moved with the length kept, another length, another
			// checksum, a file that is not this one, a size that no file holds.
			let key = u64::from(pointer.key_size);
			let value_size = u64::from(pointer.value_size);
			let lies = [
				ValuePointer {
					offset: pointer.offset + 1,
					..pointer.clone()
				},
				ValuePointer {
					offset: pointer.offset.saturating_sub(1),
					..pointer.clone()
				},
				ValuePointer {
					checksum: pointer.checksum ^ 1,
					..pointer.clone()
				},
				ValuePointer {
					key_size: pointer.key_size + 1,
					value_size: pointer.value_size.wrapping_sub(1),
					..pointer.clone()
				},
				ValuePointer {
					key_size: pointer.key_size.wrapping_sub(1),
					value_size: pointer.value_size + 1,
					..pointer.clone()
				},
				ValuePointer {
					value_size: pointer.value_size + 1,
					..pointer.clone()
				},
				ValuePointer {
					key_size: u32::MAX,
					..pointer.clone()
				},
				ValuePointer {
					value_size: u32::MAX,
					..pointer.clone()
				},
				ValuePointer {
					offset: u64::MAX,
					..pointer.clone()
				},
			];
			assert_eq!(key + value_size + 20, pointer.total_entry_size(), "{what}");
			for (i, lie) in lies.iter().enumerate() {
				let result = vlog_sealed::open_entry(&reader, lie, stored.clone());
				assert!(result.is_err(), "{what}: lie {i}");
			}
		}

		// Stored bytes of any length behind the pointer of an entry are never opened.
		let mut rng = Rng::new(4);
		let pointer = &entries[0].pointer;
		for case in 0..200 {
			let garbage = rng.bytes(pointer.total_entry_size() as usize);
			let result = vlog_sealed::open_entry(&reader, pointer, garbage);
			assert!(result.is_err(), "{suite:?}: garbage {case}, seed {SEED:#x}");
		}
	}
}

// ---------------------------------------------------------------------------
// the segments of the write-ahead log
// ---------------------------------------------------------------------------

/// Whether `found` is the start of `written`: the records that were read are the first ones, as
/// they were written.
fn is_the_start_of(found: &[Vec<u8>], written: &[Vec<u8>]) -> bool {
	found.len() <= written.len() && found.iter().zip(written).all(|(a, b)| a == b)
}

/// Reads the segment that is `bytes` as the segment `id` of the sealed log of `suite`, and says
/// what came of it: whatever is read is the start of what was written, or the read fails.
fn check_segment(
	context: &crate::wal::WalContext,
	path: &std::path::Path,
	id: u64,
	bytes: &[u8],
	written: &[Vec<u8>],
	what: &str,
) {
	fs::write(path, bytes).unwrap();
	if let Ok(found) = read_records(context, path, id) {
		assert!(
			is_the_start_of(&found, written),
			"{what}: a record was read that was not written, seed {SEED:#x}"
		);
	}
}

#[test]
fn a_sealed_segment_gives_back_the_start_of_what_was_written_or_an_error_whatever_is_done_to_it() {
	let configurations = [
		(CipherSuite::Aes256Gcm, WalCompression::None),
		(CipherSuite::XChaCha20Poly1305, WalCompression::Lz4),
	];
	for (suite, compression) in configurations {
		let dir = TempDir::new().unwrap();
		let ctx = wal_context(suite);
		let written: Vec<Vec<u8>> =
			vec![record(1, 120), record(2, 17), record(3, 300), record(4, 2)];
		let id = write_segment(dir.path(), &ctx, compression, &written);
		let wal = sealed_context(&ctx);
		let original = fs::read(segment_path(dir.path(), id)).unwrap();
		assert_eq!(read_records(&wal, &segment_path(dir.path(), id), id).unwrap(), written);

		let scratch = dir.path().join("scratch");
		let what = format!("{suite:?}, {compression:?}");

		// Cut at every length.
		for len in 0..original.len() {
			check_segment(
				&wal,
				&scratch,
				id,
				&original[..len],
				&written,
				&format!("{what}, cut {len}"),
			);
		}
		let mut rng = Rng::new(5);
		// One bit of every byte, and several bits, and a few bytes of random values, and a span of
		// zeros.
		for at in 0..original.len() {
			let changed = flipped(&original, at, rng.below(8));
			check_segment(&wal, &scratch, id, &changed, &written, &format!("{what}, flip at {at}"));
		}
		for case in 0..300 {
			let flips = 2 + rng.below(6);
			let changed = match case % 3 {
				0 => with_flips(&mut rng, &original, flips),
				1 => with_splice(&mut rng, &original, 8),
				_ => with_zeros(&mut rng, &original, 64),
			};
			check_segment(&wal, &scratch, id, &changed, &written, &format!("{what}, case {case}"));
		}
	}
}

#[test]
fn a_sealed_segment_of_records_that_span_blocks_is_read_as_far_as_it_is_whole() {
	let suite = CipherSuite::XChaCha20Poly1305;
	let dir = TempDir::new().unwrap();
	let ctx = wal_context(suite);
	// The record of 40,000 bytes is framed as a first and a last fragment across two blocks, and
	// the one after it begins in the second.
	let written: Vec<Vec<u8>> = vec![record(1, 90), record(2, 40_000), record(3, 500)];
	let id = write_segment(dir.path(), &ctx, WalCompression::None, &written);
	let wal = sealed_context(&ctx);
	let original = fs::read(segment_path(dir.path(), id)).unwrap();
	assert!(original.len() > BLOCK_SIZE, "the segment has more than a block");
	assert_eq!(read_records(&wal, &segment_path(dir.path(), id), id).unwrap(), written);
	let scratch = dir.path().join("scratch");

	// Cut around the places where the framing changes: the header record, the start of a record,
	// the end of the first block and the start of the second.
	let mut lengths = vec![0, 1, 6, 7, 8, 100, 101, 102, 107, 108, 109, 140, original.len() - 1];
	for around in [BLOCK_SIZE, original.len() - 520, original.len() - 7] {
		for delta in 0..4 {
			lengths.push(around.saturating_sub(delta));
			lengths.push(around + delta);
		}
	}
	for len in lengths {
		if len < original.len() {
			check_segment(&wal, &scratch, id, &original[..len], &written, &format!("cut {len}"));
		}
	}

	let mut rng = Rng::new(6);
	for case in 0..40 {
		let changed = match case % 4 {
			0 => with_flips(&mut rng, &original, 1),
			1 => with_splice(&mut rng, &original, 4),
			2 => with_zeros(&mut rng, &original, 2 * BLOCK_SIZE),
			_ => {
				// A zeroed block: the first block and everything in it, or the second.
				let mut changed = original.clone();
				let from = (case / 4 % 2) * BLOCK_SIZE;
				let to = (from + BLOCK_SIZE).min(changed.len());
				changed[from..to].fill(0);
				changed
			}
		};
		check_segment(&wal, &scratch, id, &changed, &written, &format!("case {case}"));
	}
}

// ---------------------------------------------------------------------------
// a table
// ---------------------------------------------------------------------------

const TABLE_ID: u64 = 7;
const KEYS: usize = 24;

fn key_of(i: usize) -> Vec<u8> {
	format!("key{i:04}").into_bytes()
}

fn value_of(i: usize) -> Vec<u8> {
	format!("value{i:04}-{}", "ab".repeat(16)).into_bytes()
}

fn options(ctx: &Arc<EncryptionContext>, compression: CompressionType) -> Arc<Options> {
	let opts = Options {
		block_size: 256,
		block_restart_interval: 4,
		index_partition_size: 100,
		compression_per_level: vec![compression],
		..Default::default()
	};
	Arc::new(opts.with_encryption_context(Arc::clone(ctx)))
}

/// A sealed table of `KEYS` keys, with more than a block of each kind but the top-level ones.
fn table_file(opts: &Arc<Options>) -> Vec<u8> {
	let mut file = Vec::new();
	{
		let mut writer = TableWriter::new(&mut file, TABLE_ID, Arc::clone(opts), 0);
		for i in 0..KEYS {
			let key = InternalKey::new(key_of(i), (i + 1) as u64, InternalKeyKind::Set);
			writer.add(key, &value_of(i)).unwrap();
		}
		writer.finish().unwrap();
	}
	file
}

/// Opens `bytes` as the table of the database of `opts`, and says what came of it: it does not
/// open, or every key reads back as it was or fails.
fn check_table(opts: &Arc<Options>, bytes: &[u8], what: &str) {
	let Ok(table) =
		Table::new(TABLE_ID, Arc::clone(opts), Arc::new(bytes.to_vec()), bytes.len() as u64)
	else {
		return;
	};
	for i in 0..KEYS {
		let lookup = InternalKey::new(key_of(i), INTERNAL_KEY_SEQ_NUM_MAX, InternalKeyKind::Set);
		match table.get(&lookup) {
			Ok(Some((key, value))) => {
				assert_eq!(key.user_key, key_of(i), "{what}: key {i}, seed {SEED:#x}");
				assert_eq!(value, value_of(i), "{what}: key {i}, seed {SEED:#x}");
			}
			Ok(None) => panic!("{what}: key {i} is missing and nothing failed, seed {SEED:#x}"),
			Err(_) => {}
		}
	}
}

#[test]
fn a_sealed_table_opens_and_reads_back_what_was_written_or_fails_whatever_is_done_to_it() {
	let configurations = [
		(CipherSuite::Aes256Gcm, CompressionType::None),
		(CipherSuite::XChaCha20Poly1305, CompressionType::SnappyCompression),
	];
	for (suite, compression) in configurations {
		let ctx = Arc::new(context(suite));
		let opts = options(&ctx, compression);
		let original = table_file(&opts);
		let what = format!("{suite:?}, {compression:?}");
		// The control: the table as it was written reads back.
		let table = Table::new(
			TABLE_ID,
			Arc::clone(&opts),
			Arc::new(original.clone()),
			original.len() as u64,
		)
		.unwrap();
		for i in 0..KEYS {
			let lookup =
				InternalKey::new(key_of(i), INTERNAL_KEY_SEQ_NUM_MAX, InternalKeyKind::Set);
			assert_eq!(table.get(&lookup).unwrap().unwrap().1, value_of(i), "{what}");
		}

		let mut rng = Rng::new(7);
		// Cut at the lengths that end a header or a footer, and at others.
		let mut lengths: Vec<usize> = (0..TABLE_FULL_FOOTER_LENGTH + HEADER_LEN + 4).collect();
		for _ in 0..60 {
			lengths.push(rng.below(original.len()));
		}
		for len in lengths {
			if len < original.len() {
				check_table(&opts, &original[..len], &format!("{what}, cut {len}"));
			}
		}

		// Every byte of the header and of the footer, which are the parts that nothing seals, and
		// of the rest one bit in a few.
		let footer = original.len() - TABLE_FULL_FOOTER_LENGTH;
		for at in (0..HEADER_LEN).chain(footer..original.len()) {
			for bit in [0, 3, 7] {
				check_table(&opts, &flipped(&original, at, bit), &format!("{what}, byte {at}"));
			}
		}
		for case in 0..150 {
			let flips = 1 + rng.below(3);
			let changed = match case % 3 {
				0 => with_flips(&mut rng, &original, flips),
				1 => with_splice(&mut rng, &original, 6),
				_ => with_zeros(&mut rng, &original, 48),
			};
			check_table(&opts, &changed, &format!("{what}, case {case}"));
		}

		// A footer that says what it likes about where the blocks are, on a table whose blocks
		// are all as they were.
		for case in 0..100 {
			let mut changed = original.clone();
			let at = footer + 2;
			let handle_len = 2 + rng.below(30);
			let handle = rng.bytes(handle_len);
			changed[at..at + handle.len()].copy_from_slice(&handle);
			check_table(&opts, &changed, &format!("{what}, footer {case}"));
		}
	}
}

#[test]
fn a_footer_and_a_block_handle_that_are_not_what_a_writer_wrote_are_refused_and_never_overrun() {
	let mut rng = Rng::new(8);
	for _ in 0..2000 {
		let len = rng.below(80);
		let bytes = rng.bytes(len);
		let _ = Footer::decode(&bytes);
		let _ = BlockHandle::decode(&bytes);
		// With the magic of a footer where it is looked for.
		if bytes.len() >= TABLE_FULL_FOOTER_LENGTH {
			let mut footer = bytes.clone();
			let end = footer.len();
			footer[end - 8..].copy_from_slice(&[0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb]);
			let _ = Footer::decode(&footer);
		}
	}
}
