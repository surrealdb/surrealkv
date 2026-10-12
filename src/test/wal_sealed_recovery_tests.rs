//! Recovery of a sealed WAL sorts what it finds into a tear, which it repairs, and everything else,
//! which it refuses without touching a file.
//!
//! The rule that these tests are about: a failure that has to do with a key, a header or a tag is
//! never repaired. For each way that one can happen, in each recovery mode, the directory of the
//! WAL is the same afterwards as before, name for name and byte for byte, and the error says what
//! the failure was. A torn tail, the ordinary result of losing power in the middle of an append, is
//! recovered as it is for a plaintext WAL: the records before it survive, and the writer goes on in
//! a segment of its own.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;

use super::wal_sealed_tests::{
	context,
	context_of,
	encoded_batch,
	fix_checksum,
	physical_records,
	read_records,
	sealed_context,
	sealed_options,
	segment_path,
	snapshot,
	DB,
	HEADER_RECORD_LEN,
	ROOT,
	SUITES,
};
use crate::error::{EncryptionError, Error};
use crate::lsm::Core;
use crate::memtable::MemTable;
use crate::tde::{CipherSuite, EncryptionContext, KeyManager, SoftwareKeyManager, HEADER_LEN};
use crate::wal::manager::Wal;
use crate::wal::parallel_recovery::decode_sealed_segments;
use crate::wal::sealed_recovery::{decode_segment, SegmentEnd};
use crate::wal::{segment_name, CompressionType, SegmentRef, WalContext, BLOCK_SIZE, HEADER_SIZE};
use crate::WalRecoveryMode;

const ARENA: usize = 1 << 20;

const MODES: [WalRecoveryMode; 2] =
	[WalRecoveryMode::AbsoluteConsistency, WalRecoveryMode::TolerateCorruptedWithRepair];

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// What recovery of a WAL gave: the highest sequence number, and every memtable that it made.
struct Recovered {
	max_seq: Option<u64>,
	memtables: Vec<Arc<MemTable>>,
}

impl Recovered {
	fn has(&self, key: &str) -> bool {
		self.memtables.iter().any(|memtable| memtable.get(key.as_bytes(), None).is_some())
	}

	/// Panics unless exactly the keys of `present` are there among `all`.
	fn assert_keys(&self, all: &[&str], present: &[&str], what: &str) {
		for key in all {
			assert_eq!(self.has(key), present.contains(key), "{what}: {key}");
		}
	}
}

/// Replays the WAL in `dir` as a tree does, with the context and the mode, and keeps what it made.
fn recover(context: &WalContext, dir: &Path, mode: WalRecoveryMode) -> crate::Result<Recovered> {
	let mut memtables = Vec::new();
	let (max_seq, active, _) =
		Core::replay_wal_in(context, dir, 0, "test", mode, ARENA, |memtable, _, _| {
			memtables.push(memtable);
			Ok(())
		})?;
	memtables.extend(active);
	Ok(Recovered {
		max_seq,
		memtables,
	})
}

/// The error of a recovery that is meant to be refused.
fn refused(context: &WalContext, dir: &Path, mode: WalRecoveryMode) -> Error {
	match recover(context, dir, mode) {
		Ok(_) => panic!("{mode:?}: the WAL was recovered, and was meant to be refused"),
		Err(error) => error,
	}
}

fn is_encryption(error: &Error, expected: &EncryptionError) -> bool {
	matches!(error, Error::Encryption(found) if found == expected)
}

fn is_gap(error: &Error) -> bool {
	matches!(error, Error::Wal(message) if message.contains("Index gap"))
}

fn is_unexpected(error: &Error) -> bool {
	matches!(error, Error::Wal(message) if message.contains("Unexpected content"))
}

type Plan = Vec<Vec<Vec<u8>>>;

/// Three segments of a few batches each.
fn standard_plan() -> Plan {
	vec![
		vec![encoded_batch(10, "a0", 2, 30), encoded_batch(20, "a1", 1, 30)],
		vec![encoded_batch(30, "b0", 1, 30), encoded_batch(40, "b1", 2, 30)],
		vec![
			encoded_batch(50, "c0", 1, 30),
			encoded_batch(60, "c1", 1, 30),
			encoded_batch(70, "c2", 1, 30),
		],
	]
}

/// Every key of the standard plan.
const KEYS: [&str; 9] = [
	"a0-key-0000",
	"a0-key-0001",
	"a1-key-0000",
	"b0-key-0000",
	"b1-key-0000",
	"b1-key-0001",
	"c0-key-0000",
	"c1-key-0000",
	"c2-key-0000",
];

/// The keys of the standard plan without those of the batch `tag`.
fn keys_without(tag: &str) -> Vec<&'static str> {
	KEYS.iter().copied().filter(|key| !key.starts_with(tag)).collect()
}

/// Writes the batches of `plan` to a sealed WAL in `dir`, the batches of each list to a segment of
/// its own, in order, a list with none making a segment with nothing in it.
fn build(dir: &Path, enc: &Arc<EncryptionContext>, compression: CompressionType, plan: &Plan) {
	let mut wal = Wal::open(dir, sealed_options(enc, compression)).unwrap();
	for (n, batches) in plan.iter().enumerate() {
		if n > 0 || batches.is_empty() {
			wal.rotate().unwrap();
		}
		for batch in batches {
			wal.append(batch).unwrap();
		}
	}
	wal.close().unwrap();
}

/// A directory with the files of `template`, to be spoiled in a way of its own.
fn work_copy(template: &Path) -> TempDir {
	let dir = TempDir::new().unwrap();
	for entry in fs::read_dir(template).unwrap() {
		let path = entry.unwrap().path();
		fs::copy(&path, dir.path().join(path.file_name().unwrap())).unwrap();
	}
	dir
}

fn read(dir: &Path, id: u64) -> Vec<u8> {
	fs::read(segment_path(dir, id)).unwrap()
}

fn write(dir: &Path, id: u64, bytes: &[u8]) {
	fs::write(segment_path(dir, id), bytes).unwrap();
}

fn salt(bytes: &[u8]) -> Vec<u8> {
	bytes[HEADER_SIZE + 40..HEADER_SIZE + 72].to_vec()
}

/// Every recovery mode must refuse the WAL in `template` as spoiled by `spoil`, with an error that
/// `says`, and leave the directory as it was.
fn assert_refused_everywhere(
	template: &Path,
	context: &WalContext,
	what: &str,
	spoil: impl Fn(&Path),
	says: impl Fn(&Error) -> bool,
) {
	for mode in MODES {
		let dir = work_copy(template);
		spoil(dir.path());
		let before = snapshot(dir.path());
		let error = refused(context, dir.path(), mode);
		assert!(says(&error), "{what}, {mode:?}: {error}");
		assert_eq!(snapshot(dir.path()), before, "{what}, {mode:?}: the WAL was changed");
		assert!(!dir.path().join("repair_temp").exists(), "{what}, {mode:?}: a repair began");
	}
}

// ---------------------------------------------------------------------------
// keys: never a tear
// ---------------------------------------------------------------------------

#[test]
fn a_key_that_does_not_fit_is_refused_in_every_mode_and_nothing_changes() {
	for suite in SUITES {
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &standard_plan());

		let cases = [
			(
				"another root",
				sealed_context(&context_of([0x99; 32], DB, suite)),
				EncryptionError::WrongKey,
			),
			(
				"another database",
				sealed_context(&context_of(ROOT, [0xD1; 16], suite)),
				EncryptionError::DbIdMismatch,
			),
			("no key", WalContext::plaintext(), EncryptionError::NotConfigured),
		];
		for (what, wrong, expected) in cases {
			assert_refused_everywhere(
				template.path(),
				&wrong,
				&format!("{suite:?}, {what}"),
				|_| {},
				|error| is_encryption(error, &expected),
			);
		}

		// The right key recovers all of it, and a context of the other suite does as well: the
		// suite of a segment is the one in its header.
		let other_suite = match suite {
			CipherSuite::Aes256Gcm => CipherSuite::XChaCha20Poly1305,
			_ => CipherSuite::Aes256Gcm,
		};
		for reader in [enc, context_of(ROOT, DB, other_suite)] {
			let dir = work_copy(template.path());
			let recovered =
				recover(&sealed_context(&reader), dir.path(), WalRecoveryMode::AbsoluteConsistency)
					.unwrap();
			recovered.assert_keys(&KEYS, &KEYS, "the right key");
			assert_eq!(recovered.max_seq, Some(70));
		}
	}
}

#[test]
fn a_key_that_was_retired_is_refused_for_what_it_is_and_nothing_changes() {
	let template = TempDir::new().unwrap();
	let manager = Arc::new(SoftwareKeyManager::new(ROOT));
	let enc = Arc::new(EncryptionContext::new(
		Arc::clone(&manager) as Arc<dyn KeyManager>,
		crate::tde::DbId::from_bytes(DB),
		CipherSuite::Aes256Gcm,
	));
	build(template.path(), &enc, CompressionType::None, &standard_plan());
	manager.add_key(2, [0x22; 32]).unwrap();
	manager.set_active_key_id(2).unwrap();
	manager.retire_key(1).unwrap();
	assert_refused_everywhere(
		template.path(),
		&sealed_context(&enc),
		"a retired key",
		|_| {},
		|error| is_encryption(error, &EncryptionError::KeyRetired),
	);
}

/// The tail of the last segment was cut, which the right key repairs in the default mode. Whatever
/// else a recovery finds, a key that does not fit is found first, and nothing is repaired for it.
#[test]
fn a_torn_tail_is_not_repaired_by_a_wal_whose_key_does_not_fit() {
	for suite in SUITES {
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &standard_plan());
		let cut = |dir: &Path| {
			let bytes = read(dir, 2);
			let (at, length) = *batch_records(&bytes).last().unwrap();
			write(dir, 2, &bytes[..at + HEADER_SIZE + length / 2]);
		};

		let cases = [
			(
				"another root",
				sealed_context(&context_of([0x99; 32], DB, suite)),
				EncryptionError::WrongKey,
			),
			(
				"another database",
				sealed_context(&context_of(ROOT, [0xD1; 16], suite)),
				EncryptionError::DbIdMismatch,
			),
			("no key", WalContext::plaintext(), EncryptionError::NotConfigured),
		];
		for (what, wrong, expected) in cases {
			assert_refused_everywhere(
				template.path(),
				&wrong,
				&format!("{suite:?}, {what}, with a cut tail"),
				cut,
				|error| is_encryption(error, &expected),
			);
		}

		// With the right key the same tail is a tear, and the default mode repairs it.
		let dir = work_copy(template.path());
		cut(dir.path());
		let recovered = recover(
			&sealed_context(&enc),
			dir.path(),
			WalRecoveryMode::TolerateCorruptedWithRepair,
		)
		.unwrap();
		recovered.assert_keys(&KEYS, &keys_without("c2"), "the right key");
	}
}

/// A segment that fails to authenticate decides the recovery, wherever the tear is: the tear of the
/// last segment is not repaired when an earlier segment is refused, and a tear after a record that
/// fails to authenticate is not one.
#[test]
fn a_tear_is_not_repaired_when_a_record_before_it_fails_to_authenticate() {
	for suite in SUITES {
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &standard_plan());
		let context = sealed_context(&enc);

		let cut_last = |dir: &Path| {
			let bytes = read(dir, 2);
			let (at, length) = *batch_records(&bytes).last().unwrap();
			write(dir, 2, &bytes[..at + HEADER_SIZE + length / 2]);
		};
		// A record of a segment, changed, with its checksum made again.
		let change = |dir: &Path, segment: u64, which: usize| {
			let mut bytes = read(dir, segment);
			let (at, _) = batch_records(&bytes)[which];
			bytes[at + HEADER_SIZE + 3] ^= 0x01;
			fix_checksum(&mut bytes, at);
			write(dir, segment, &bytes);
		};

		assert_refused_everywhere(
			template.path(),
			&context,
			&format!("{suite:?}, a cut last segment after a changed record in the one before"),
			|dir| {
				change(dir, 1, 1);
				cut_last(dir);
			},
			|error| is_encryption(error, &EncryptionError::AuthenticationFailed),
		);
		assert_refused_everywhere(
			template.path(),
			&context,
			&format!("{suite:?}, a cut last record after a changed record of the same segment"),
			|dir| {
				change(dir, 2, 0);
				cut_last(dir);
			},
			|error| is_encryption(error, &EncryptionError::AuthenticationFailed),
		);
	}
}

/// A WAL that has no key is not one that may repair a sealed segment, whatever else is wrong with
/// it: the plaintext repair deletes a segment that holds no valid record, and a sealed segment
/// whose first bytes rotted holds none that a plaintext reader can read. What tells it apart is the
/// start of the encryption header in its first record, which a rotted type byte leaves in place.
#[test]
fn a_sealed_segment_whose_type_byte_rotted_is_still_refused_by_a_wal_that_has_no_key() {
	for suite in SUITES {
		// One segment, so that nothing but the rot decides what a reader without a key makes of it.
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		let plan: Plan = vec![standard_plan().remove(0)];
		build(template.path(), &enc, CompressionType::None, &plan);
		let keyless = WalContext::plaintext();

		for rotted in [0x0B_u8, 0x00, 0x01, 0xFF] {
			let spoil = |dir: &Path| {
				let mut bytes = read(dir, 0);
				bytes[6] = rotted;
				write(dir, 0, &bytes);
			};
			assert_refused_everywhere(
				template.path(),
				&keyless,
				&format!("{suite:?}, type byte {rotted:#x}"),
				spoil,
				|error| is_encryption(error, &EncryptionError::NotConfigured),
			);

			// So is the open of the WAL for appending, which builds a plaintext writer on the file
			// that it finds last, whatever the type byte says it is.
			let dir = work_copy(template.path());
			spoil(dir.path());
			let before = snapshot(dir.path());
			let opened =
				Wal::open_with_min_log_number(dir.path(), 0, crate::wal::Options::default());
			assert!(
				matches!(
					opened.err(),
					Some(crate::wal::Error::Encryption(EncryptionError::NotConfigured))
				),
				"{suite:?}, type byte {rotted:#x}"
			);
			assert_eq!(snapshot(dir.path()), before, "{suite:?}, type byte {rotted:#x}");

			// The repair is refused as well, on its own: it is where the deletion would be.
			let dir = work_copy(template.path());
			spoil(dir.path());
			let before = snapshot(dir.path());
			let error = crate::wal::recovery::repair_corrupted_wal_segment(dir.path(), 0, 0)
				.expect_err("a keyless repair of a sealed segment must be refused");
			assert!(is_encryption(&error, &EncryptionError::NotConfigured), "{error}");
			assert_eq!(snapshot(dir.path()), before, "{suite:?}, type byte {rotted:#x}");
			assert!(!dir.path().join("repair_temp").exists());
		}
	}
}

// ---------------------------------------------------------------------------
// tags: a record that was changed
// ---------------------------------------------------------------------------

/// The records of a segment that hold batches: where each begins and the length of its payload.
fn batch_records(bytes: &[u8]) -> Vec<(usize, usize)> {
	physical_records(bytes)
		.into_iter()
		.filter(|(_, kind, _)| *kind == 1)
		.map(|(at, _, length)| (at, length))
		.collect()
}

#[test]
fn a_record_that_was_changed_and_whose_checksum_was_made_again_is_an_authentication_failure() {
	for suite in SUITES {
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &standard_plan());
		let context = sealed_context(&enc);

		for segment in 0..3u64 {
			let records = batch_records(&read(template.path(), segment));
			// The first and the last record of every segment, the last segment included: that is
			// where a repair would go.
			for which in [0, records.len() - 1] {
				let (at, length) = records[which];
				// The first and the last byte of the ciphertext, the first and the last of the tag.
				for offset in [0, length - 17, length - 16, length - 1] {
					assert_refused_everywhere(
						template.path(),
						&context,
						&format!("{suite:?}, segment {segment}, record {which}, byte {offset}"),
						|dir| {
							let mut bytes = read(dir, segment);
							bytes[at + HEADER_SIZE + offset] ^= 0x01;
							fix_checksum(&mut bytes, at);
							write(dir, segment, &bytes);
						},
						|error| is_encryption(error, &EncryptionError::AuthenticationFailed),
					);
				}
			}
		}
	}
}

#[test]
fn a_record_that_is_sealed_at_another_index_or_in_another_segment_does_not_authenticate() {
	for suite in SUITES {
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &standard_plan());
		let context = sealed_context(&enc);

		// The first two records of the last segment change places: each is intact, with its
		// checksum, and each is at an index that is not its own.
		assert_refused_everywhere(
			template.path(),
			&context,
			&format!("{suite:?}, records that changed places"),
			|dir| {
				let bytes = read(dir, 2);
				let records = batch_records(&bytes);
				let (first, second) = (records[0], records[1]);
				let end = |(at, length): (usize, usize)| at + HEADER_SIZE + length;
				assert_eq!(end(first), second.0, "the records follow each other");
				let mut swapped = bytes[..first.0].to_vec();
				swapped.extend_from_slice(&bytes[second.0..end(second)]);
				swapped.extend_from_slice(&bytes[first.0..end(first)]);
				swapped.extend_from_slice(&bytes[end(second)..]);
				write(dir, 2, &swapped);
			},
			|error| is_encryption(error, &EncryptionError::AuthenticationFailed),
		);

		// A record that is there twice: the second is at an index that the first took.
		assert_refused_everywhere(
			template.path(),
			&context,
			&format!("{suite:?}, a repeated record"),
			|dir| {
				let bytes = read(dir, 1);
				let (at, length) = batch_records(&bytes)[0];
				let end = at + HEADER_SIZE + length;
				let mut repeated = bytes[..end].to_vec();
				repeated.extend_from_slice(&bytes[at..end]);
				repeated.extend_from_slice(&bytes[end..]);
				write(dir, 1, &repeated);
			},
			|error| is_encryption(error, &EncryptionError::AuthenticationFailed),
		);

		// A record of one segment in another: the key is another, and so is the segment.
		assert_refused_everywhere(
			template.path(),
			&context,
			&format!("{suite:?}, a record of another segment"),
			|dir| {
				let donor = read(dir, 1);
				let (donor_at, donor_length) = batch_records(&donor)[0];
				let bytes = read(dir, 2);
				let (at, length) = batch_records(&bytes)[0];
				assert_eq!(donor_length, length, "records of the same size, so that the rest fits");
				let mut moved = bytes;
				moved[at..at + HEADER_SIZE + length]
					.copy_from_slice(&donor[donor_at..donor_at + HEADER_SIZE + length]);
				write(dir, 2, &moved);
			},
			|error| is_encryption(error, &EncryptionError::AuthenticationFailed),
		);
	}
}

// ---------------------------------------------------------------------------
// the header
// ---------------------------------------------------------------------------

/// How a byte of the header is changed.
#[derive(Clone, Copy)]
enum Change {
	Xor(u8),
	Set(u8),
}

impl Change {
	fn apply(self, byte: u8) -> u8 {
		match self {
			Change::Xor(mask) => byte ^ mask,
			Change::Set(value) => value,
		}
	}
}

/// Makes the checksum of the encryption header, which is inside the record, and the checksum of the
/// record again, as someone who changed a field and has no key can.
fn refresh_checksums(bytes: &mut [u8]) {
	let crc = crc32fast::hash(&bytes[HEADER_SIZE..HEADER_SIZE + HEADER_LEN - 4]);
	bytes[HEADER_SIZE + HEADER_LEN - 4..HEADER_SIZE + HEADER_LEN]
		.copy_from_slice(&crc.to_be_bytes());
	fix_checksum(bytes, 0);
}

#[test]
fn a_field_of_the_header_that_was_changed_is_refused_for_what_it_is_in_every_mode() {
	for suite in SUITES {
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &standard_plan());
		let context = sealed_context(&enc);
		let other_suite = 3 - suite.id();

		// The offset is in the encryption header, which is the payload of the first record. What
		// each says is decided by the order in which the header is checked: its form, then
		// what it is for, then the key, and only the tag can tell that a field that fits
		// was changed.
		let cases = [
			("magic", 0, Change::Xor(1), EncryptionError::MalformedHeader),
			("version", 4, Change::Set(2), EncryptionError::UnsupportedVersion),
			("suite, the other one", 5, Change::Set(other_suite), EncryptionError::WrongKey),
			("suite, the reserved one", 5, Change::Set(3), EncryptionError::UnsupportedSuite),
			("kind, a table", 6, Change::Set(1), EncryptionError::FileIdMismatch),
			("kind, unknown", 6, Change::Set(9), EncryptionError::UnsupportedVersion),
			("reserved", 7, Change::Set(1), EncryptionError::UnsupportedVersion),
			("key id", 11, Change::Set(9), EncryptionError::UnknownKeyId),
			("wrapped key length", 12, Change::Set(1), EncryptionError::UnsupportedVersion),
			("database", 16, Change::Xor(1), EncryptionError::DbIdMismatch),
			("segment", 39, Change::Xor(1), EncryptionError::FileIdMismatch),
			("salt", 40, Change::Xor(1), EncryptionError::WrongKey),
			("salt, last byte", 71, Change::Xor(0x80), EncryptionError::WrongKey),
			("tag", 72, Change::Xor(1), EncryptionError::WrongKey),
			("tag, last byte", 87, Change::Xor(1), EncryptionError::WrongKey),
			("format of the segment", HEADER_LEN, Change::Xor(1), EncryptionError::WrongKey),
			(
				"compression of the segment",
				HEADER_LEN + 1,
				Change::Xor(1),
				EncryptionError::WrongKey,
			),
		];
		// In the last segment, which is where a repair would go, and in the first.
		for segment in [2u64, 0] {
			for (what, offset, change, expected) in &cases {
				let (offset, change) = (*offset, *change);
				assert_refused_everywhere(
					template.path(),
					&context,
					&format!("{suite:?}, segment {segment}, {what}"),
					|dir| {
						let mut bytes = read(dir, segment);
						bytes[HEADER_SIZE + offset] = change.apply(bytes[HEADER_SIZE + offset]);
						refresh_checksums(&mut bytes);
						write(dir, segment, &bytes);
					},
					|error| is_encryption(error, expected),
				);
			}

			// A salt of zeros is not a salt.
			assert_refused_everywhere(
				template.path(),
				&context,
				&format!("{suite:?}, segment {segment}, a salt of zeros"),
				|dir| {
					let mut bytes = read(dir, segment);
					bytes[HEADER_SIZE + 40..HEADER_SIZE + 72].fill(0);
					refresh_checksums(&mut bytes);
					write(dir, segment, &bytes);
				},
				|error| is_encryption(error, &EncryptionError::MalformedHeader),
			);
		}
	}
}

#[test]
fn a_header_that_was_taken_from_another_segment_names_that_segment_and_is_refused() {
	for suite in SUITES {
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &standard_plan());
		// The file of segment 1 is the file of segment 0: a copy under another name.
		assert_refused_everywhere(
			template.path(),
			&sealed_context(&enc),
			&format!("{suite:?}, a segment under another number"),
			|dir| write(dir, 1, &read(dir, 0)),
			|error| is_encryption(error, &EncryptionError::FileIdMismatch),
		);
	}
}

#[test]
fn a_header_that_was_damaged_is_damage_only_where_nothing_authenticated_follows_it() {
	for suite in SUITES {
		let enc = context(suite);
		let context = sealed_context(&enc);

		// A segment with batches: the bytes after the header are there, so this is not what a crash
		// leaves, and it is refused as such whatever the mode is.
		let template = TempDir::new().unwrap();
		build(template.path(), &enc, CompressionType::None, &standard_plan());
		assert_refused_everywhere(
			template.path(),
			&context,
			&format!("{suite:?}, a damaged header before batches"),
			|dir| {
				let mut bytes = read(dir, 2);
				bytes[HEADER_SIZE + 45] ^= 0x01;
				write(dir, 2, &bytes);
			},
			is_gap,
		);

		// A segment with a header and nothing else, which is what a crash during its creation
		// leaves.
		let mut plan = standard_plan();
		plan.push(Vec::new());
		let template = TempDir::new().unwrap();
		build(template.path(), &enc, CompressionType::None, &plan);
		assert_eq!(read(template.path(), 3).len(), HEADER_RECORD_LEN);
		let damage = |dir: &Path| {
			let mut bytes = read(dir, 3);
			bytes[HEADER_SIZE + 45] ^= 0x01;
			write(dir, 3, &bytes);
		};

		let absolute = work_copy(template.path());
		damage(absolute.path());
		let before = snapshot(absolute.path());
		let error = refused(&context, absolute.path(), WalRecoveryMode::AbsoluteConsistency);
		assert!(
			matches!(
				error,
				Error::WalCorruption {
					segment_id: 3,
					offset: 0,
					..
				}
			),
			"{suite:?}: {error}"
		);
		assert_eq!(snapshot(absolute.path()), before);

		let tolerant = work_copy(template.path());
		damage(tolerant.path());
		let damaged = read(tolerant.path(), 3);
		let recovered =
			recover(&context, tolerant.path(), WalRecoveryMode::TolerateCorruptedWithRepair)
				.unwrap();
		recovered.assert_keys(&KEYS, &KEYS, "every batch is before the damage");
		// The damaged segment is kept as it was, and a segment with a header of its own is in its
		// place, under a salt of its own.
		let kept = format!("{}.corrupt", segment_name(3, "wal"));
		assert_eq!(fs::read(tolerant.path().join(kept)).unwrap(), damaged, "{suite:?}");
		let repaired = read(tolerant.path(), 3);
		assert_eq!(repaired.len(), HEADER_RECORD_LEN);
		assert_ne!(salt(&repaired), salt(&damaged));
		assert!(read_records(&context, &segment_path(tolerant.path(), 3), 3).unwrap().is_empty());
		assert!(!tolerant.path().join("repair_temp").exists());
	}
}

// ---------------------------------------------------------------------------
// what a segment may hold
// ---------------------------------------------------------------------------

#[test]
fn a_plaintext_segment_in_a_sealed_wal_is_refused_in_every_mode_and_nothing_changes() {
	for suite in SUITES {
		for compression in [CompressionType::None, CompressionType::Lz4] {
			let template = TempDir::new().unwrap();
			let enc = context(suite);
			build(template.path(), &enc, CompressionType::None, &standard_plan());

			// A plaintext segment of the same number, written by a WAL that is not sealed.
			let plain = TempDir::new().unwrap();
			let options = crate::wal::Options::default().with_compression(compression);
			let mut wal = Wal::open_with_min_log_number(plain.path(), 2, options).unwrap();
			wal.append(&encoded_batch(99, "plain", 1, 30)).unwrap();
			wal.close().unwrap();
			let plaintext = read(plain.path(), 2);

			assert_refused_everywhere(
				template.path(),
				&sealed_context(&enc),
				&format!("{suite:?}, {compression:?}"),
				|dir| write(dir, 2, &plaintext),
				|error| is_encryption(error, &EncryptionError::PlaintextInEncryptedDb),
			);
		}
	}
}

/// A physical record as a writer frames it: checksum, length, type, payload.
fn framed(kind: u8, payload: &[u8]) -> Vec<u8> {
	let crc = crate::wal::calculate_crc32(&[kind], payload);
	let mut record = crc.to_be_bytes().to_vec();
	record.extend_from_slice(&(payload.len() as u16).to_be_bytes());
	record.push(kind);
	record.extend_from_slice(payload);
	record
}

#[test]
fn a_record_that_a_sealed_segment_does_not_hold_is_refused_even_with_a_good_checksum() {
	for suite in SUITES {
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &standard_plan());
		let context = sealed_context(&enc);

		let header = read(template.path(), 2)[..HEADER_RECORD_LEN].to_vec();
		let injected: [(&str, Vec<u8>); 5] = [
			// The record that says how a plaintext segment is compressed: it is not authenticated,
			// so a sealed segment does not take it from anyone.
			("a compression record", framed(9, &[1])),
			("a second header", header),
			("a type that is not assigned", framed(7, b"x")),
			// The middle of a record, where none began.
			("a middle fragment", framed(3, b"abc")),
			("a last fragment", framed(4, b"abc")),
		];
		for (what, record) in injected {
			// After the header, and after the first batch.
			for after in ["the header", "the first batch"] {
				assert_refused_everywhere(
					template.path(),
					&context,
					&format!("{suite:?}, {what} after {after}"),
					|dir| {
						let bytes = read(dir, 2);
						let at = if after == "the header" {
							HEADER_RECORD_LEN
						} else {
							let (first, length) = batch_records(&bytes)[0];
							first + HEADER_SIZE + length
						};
						let mut changed = bytes[..at].to_vec();
						changed.extend_from_slice(&record);
						changed.extend_from_slice(&bytes[at..]);
						write(dir, 2, &changed);
					},
					is_unexpected,
				);
			}
		}

		// With a bad checksum, the same bytes are not known to have been written on purpose.
		let template_with_damage = work_copy(template.path());
		let mut bytes = read(template_with_damage.path(), 2);
		let mut damaged = framed(9, &[1]);
		damaged[0] ^= 0xFF;
		let mut changed = bytes[..HEADER_RECORD_LEN].to_vec();
		changed.extend_from_slice(&damaged);
		changed.extend_from_slice(&bytes[HEADER_RECORD_LEN..]);
		bytes = changed;
		write(template_with_damage.path(), 2, &bytes);
		// A record with a bad checksum followed by records that pass theirs: damage in the middle.
		for mode in MODES {
			let dir = work_copy(template_with_damage.path());
			let before = snapshot(dir.path());
			let error = refused(&context, dir.path(), mode);
			assert!(is_gap(&error), "{suite:?}, {mode:?}: {error}");
			assert_eq!(snapshot(dir.path()), before);
		}
	}
}

// ---------------------------------------------------------------------------
// gaps
// ---------------------------------------------------------------------------

/// Three segments of which the middle one is long: a batch of 150 KB between two small ones.
fn plan_with_a_long_segment() -> Plan {
	vec![
		vec![encoded_batch(10, "a0", 1, 30)],
		vec![
			encoded_batch(20, "m0", 1, 30),
			encoded_batch(30, "mb", 1, 150_000),
			encoded_batch(40, "m1", 1, 30),
		],
		vec![encoded_batch(50, "c0", 1, 30)],
	]
}

#[test]
fn damage_in_the_middle_of_a_segment_is_a_gap_and_not_a_tear() {
	for suite in SUITES {
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &plan_with_a_long_segment());
		let context = sealed_context(&enc);
		let long = read(template.path(), 1);
		let blocks = long.len().div_ceil(BLOCK_SIZE);
		assert!(blocks >= 5, "the segment spans {blocks} blocks");

		// A block that was zeroed, in the middle of the segment and at its start.
		for block in [0, 2] {
			assert_refused_everywhere(
				template.path(),
				&context,
				&format!("{suite:?}, block {block} zeroed"),
				|dir| {
					let mut bytes = read(dir, 1);
					bytes[block * BLOCK_SIZE..(block + 1) * BLOCK_SIZE].fill(0);
					write(dir, 1, &bytes);
				},
				is_gap,
			);
		}

		// A block that was changed, so that its checksum fails, with blocks after it that pass.
		assert_refused_everywhere(
			template.path(),
			&context,
			&format!("{suite:?}, a block that was changed"),
			|dir| {
				let mut bytes = read(dir, 1);
				bytes[2 * BLOCK_SIZE + 100] ^= 0x01;
				write(dir, 1, &bytes);
			},
			is_gap,
		);
	}
}

#[test]
fn a_record_that_rotted_with_a_record_after_it_is_a_gap_and_the_last_record_alone_is_a_tear() {
	for suite in SUITES {
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &standard_plan());
		let context = sealed_context(&enc);
		let records = batch_records(&read(template.path(), 2));

		// The middle record of the last segment, with the last one after it, in the same block.
		let (at, _) = records[1];
		assert_refused_everywhere(
			template.path(),
			&context,
			&format!("{suite:?}, a middle record that rotted"),
			|dir| {
				let mut bytes = read(dir, 2);
				bytes[at + HEADER_SIZE + 3] ^= 0x01;
				write(dir, 2, &bytes);
			},
			is_gap,
		);

		// The last record of the last segment has nothing after it: that is where a write is cut.
		let (at, _) = records[2];
		let dir = work_copy(template.path());
		let mut bytes = read(dir.path(), 2);
		bytes[at + HEADER_SIZE + 3] ^= 0x01;
		write(dir.path(), 2, &bytes);
		let before = snapshot(dir.path());
		let error = refused(&context, dir.path(), WalRecoveryMode::AbsoluteConsistency);
		assert!(
			matches!(
				error,
				Error::WalCorruption {
					segment_id: 2,
					..
				}
			),
			"{suite:?}: {error}"
		);
		assert_eq!(snapshot(dir.path()), before);
		let recovered =
			recover(&context, dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair).unwrap();
		recovered.assert_keys(&KEYS, &keys_without("c2"), "rot in the last record");
	}
}

#[test]
fn a_segment_that_is_missing_from_the_sequence_is_a_gap() {
	for suite in SUITES {
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &standard_plan());
		let context = sealed_context(&enc);

		// The middle one, and the first.
		for missing in [1u64, 0] {
			assert_refused_everywhere(
				template.path(),
				&context,
				&format!("{suite:?}, segment {missing} deleted"),
				|dir| fs::remove_file(segment_path(dir, missing)).unwrap(),
				is_gap,
			);
		}
	}
}

#[test]
fn a_tear_followed_by_a_segment_with_records_is_a_gap_in_every_mode() {
	for suite in SUITES {
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &standard_plan());
		let context = sealed_context(&enc);

		// The first segment ends in a write that was cut, and the segments after it hold batches.
		assert_refused_everywhere(
			template.path(),
			&context,
			&format!("{suite:?}, a cut segment before others with records"),
			|dir| {
				let bytes = read(dir, 0);
				let (at, length) = *batch_records(&bytes).last().unwrap();
				write(dir, 0, &bytes[..at + HEADER_SIZE + length / 2]);
			},
			is_gap,
		);
	}
}

// ---------------------------------------------------------------------------
// tears
// ---------------------------------------------------------------------------

/// What a repair of the last segment must have left, given the batch `torn` that was cut off it.
fn assert_repaired(
	dir: &Path,
	context: &WalContext,
	cut: &[u8],
	original_salt: &[u8],
	kept_batches: &[Vec<u8>],
	what: &str,
) {
	let corrupt = dir.join(format!("{}.corrupt", segment_name(2, "wal")));
	assert_eq!(fs::read(&corrupt).unwrap(), cut, "{what}: the segment as it was is kept");
	assert!(!dir.join("repair_temp").exists(), "{what}");
	let repaired = read(dir, 2);
	assert_ne!(
		salt(&repaired),
		original_salt,
		"{what}: the repaired segment has a salt of its own"
	);
	let read_back = read_records(context, &segment_path(dir, 2), 2).unwrap();
	assert_eq!(read_back, kept_batches, "{what}: the batches before the tear");
}

#[test]
fn a_torn_tail_of_the_last_segment_is_repaired_and_what_is_before_it_survives() {
	for suite in SUITES {
		for compression in [CompressionType::None, CompressionType::Lz4] {
			let plan = standard_plan();
			let template = TempDir::new().unwrap();
			let enc = context(suite);
			build(template.path(), &enc, compression, &plan);
			let context = sealed_context(&enc);

			let whole = read(template.path(), 2);
			let (torn_at, torn_payload) = *batch_records(&whole).last().unwrap();
			let torn_len = HEADER_SIZE + torn_payload;
			let kept = &plan[2][..2];
			let original_salt = salt(&whole);

			// From a cut that is exactly at the end of the record before, to one byte short of the
			// end, across the header and the payload.
			let cuts = [0, 1, 3, 6, 7, 8, 12, torn_len / 2, torn_len - 1];
			for cut in cuts {
				let what = format!("{suite:?}, {compression:?}, {cut} bytes of the last record");
				let cut_bytes = whole[..torn_at + cut].to_vec();

				// Absolute consistency: a clean end is taken, and a tear is refused and left alone.
				let absolute = work_copy(template.path());
				write(absolute.path(), 2, &cut_bytes);
				let before = snapshot(absolute.path());
				if cut == 0 {
					let recovered =
						recover(&context, absolute.path(), WalRecoveryMode::AbsoluteConsistency)
							.unwrap();
					recovered.assert_keys(&KEYS, &keys_without("c2"), &what);
				} else {
					let error =
						refused(&context, absolute.path(), WalRecoveryMode::AbsoluteConsistency);
					assert!(
						matches!(
							&error,
							Error::WalCorruption { segment_id: 2, offset, .. } if *offset == torn_at
						),
						"{what}: {error}"
					);
				}
				assert_eq!(snapshot(absolute.path()), before, "{what}: absolute consistency");

				// The default: the batches before the tear survive, and the segment is repaired.
				let dir = work_copy(template.path());
				write(dir.path(), 2, &cut_bytes);
				let recovered =
					recover(&context, dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair)
						.unwrap();
				recovered.assert_keys(&KEYS, &keys_without("c2"), &what);
				assert_eq!(recovered.max_seq, Some(60), "{what}");
				if cut == 0 {
					assert_eq!(snapshot(dir.path()), before, "{what}: there was nothing to repair");
					continue;
				}
				assert_repaired(dir.path(), &context, &cut_bytes, &original_salt, kept, &what);

				// The repaired WAL is whole: any mode recovers it, and changes nothing.
				let settled = snapshot(dir.path());
				let again =
					recover(&context, dir.path(), WalRecoveryMode::AbsoluteConsistency).unwrap();
				again.assert_keys(&KEYS, &keys_without("c2"), &what);
				assert_eq!(snapshot(dir.path()), settled, "{what}: a second recovery");

				// And commits go on: a WAL opened on it writes to the next segment, and everything
				// is there at the next recovery.
				let mut wal =
					Wal::open_with_min_log_number(dir.path(), 0, sealed_options(&enc, compression))
						.unwrap();
				assert_eq!(wal.get_active_log_number(), 3, "{what}");
				wal.append(&encoded_batch(80, "n0", 1, 30)).unwrap();
				wal.close().unwrap();
				let later =
					recover(&context, dir.path(), WalRecoveryMode::AbsoluteConsistency).unwrap();
				later.assert_keys(&KEYS, &keys_without("c2"), &what);
				assert!(later.has("n0-key-0000"), "{what}");
				assert_eq!(later.max_seq, Some(80), "{what}");
			}
		}
	}
}

#[test]
fn a_second_repair_of_a_segment_keeps_both_damaged_copies() {
	let suite = CipherSuite::Aes256Gcm;
	let template = TempDir::new().unwrap();
	let enc = context(suite);
	build(template.path(), &enc, CompressionType::None, &standard_plan());
	let context = sealed_context(&enc);

	let dir = work_copy(template.path());
	let first_cut = {
		let bytes = read(dir.path(), 2);
		let (at, length) = *batch_records(&bytes).last().unwrap();
		bytes[..at + HEADER_SIZE + length / 2].to_vec()
	};
	write(dir.path(), 2, &first_cut);
	recover(&context, dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair).unwrap();

	// The repaired segment is cut again, in its last record.
	let second_cut = {
		let bytes = read(dir.path(), 2);
		let (at, length) = *batch_records(&bytes).last().unwrap();
		bytes[..at + HEADER_SIZE + length / 2].to_vec()
	};
	write(dir.path(), 2, &second_cut);
	let recovered =
		recover(&context, dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair).unwrap();
	recovered.assert_keys(
		&KEYS,
		&keys_without("c1").iter().copied().filter(|k| !k.starts_with("c2")).collect::<Vec<_>>(),
		"after two repairs",
	);

	let first_copy = dir.path().join(format!("{}.corrupt", segment_name(2, "wal")));
	let second_copy = dir.path().join(format!("{}.corrupt.1", segment_name(2, "wal")));
	assert_eq!(fs::read(first_copy).unwrap(), first_cut);
	assert_eq!(fs::read(second_copy).unwrap(), second_cut);
}

#[test]
fn a_record_that_spans_blocks_is_cut_at_the_end_of_a_block_or_inside_one() {
	for suite in SUITES {
		let plan: Plan = vec![
			vec![encoded_batch(10, "p0", 1, 30)],
			vec![encoded_batch(20, "q0", 1, 30), encoded_batch(30, "qb", 1, 100_000)],
		];
		let all = ["p0-key-0000", "q0-key-0000", "qb-key-0000"];
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &plan);
		let context = sealed_context(&enc);
		let whole = read(template.path(), 1);
		assert!(whole.len() > 3 * BLOCK_SIZE);

		// At the end of a block, the rest of the record is in a block that was never written: that
		// is a clean end. Anywhere else the record is cut in a block.
		for (cut, is_tear) in [
			(BLOCK_SIZE, false),
			(BLOCK_SIZE + 3, true),
			(BLOCK_SIZE + 7, true),
			(BLOCK_SIZE + 1000, true),
			(2 * BLOCK_SIZE, false),
			(2 * BLOCK_SIZE + 5, true),
			(whole.len() - 1, true),
		] {
			let what = format!("{suite:?}, cut at {cut}");
			let cut_bytes = whole[..cut].to_vec();

			let absolute = work_copy(template.path());
			write(absolute.path(), 1, &cut_bytes);
			let before = snapshot(absolute.path());
			let result = recover(&context, absolute.path(), WalRecoveryMode::AbsoluteConsistency);
			if is_tear {
				let error = result.err().expect("a tear is refused");
				assert!(
					matches!(
						error,
						Error::WalCorruption {
							segment_id: 1,
							..
						}
					),
					"{what}: {error}"
				);
			} else {
				result.unwrap().assert_keys(&all, &["p0-key-0000", "q0-key-0000"], &what);
			}
			assert_eq!(snapshot(absolute.path()), before, "{what}");

			let dir = work_copy(template.path());
			write(dir.path(), 1, &cut_bytes);
			let recovered =
				recover(&context, dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair)
					.unwrap();
			recovered.assert_keys(&all, &["p0-key-0000", "q0-key-0000"], &what);
			// What is left is whole.
			let again =
				recover(&context, dir.path(), WalRecoveryMode::AbsoluteConsistency).unwrap();
			again.assert_keys(&all, &["p0-key-0000", "q0-key-0000"], &what);
		}
	}
}

#[test]
fn a_last_block_that_was_zeroed_is_the_end_of_the_log() {
	for suite in SUITES {
		let plan: Plan =
			vec![vec![encoded_batch(10, "p0", 1, 30), encoded_batch(20, "pb", 1, 100_000)]];
		let all = ["p0-key-0000", "pb-key-0000"];
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &plan);
		let context = sealed_context(&enc);
		let whole = read(template.path(), 0);
		let last_block = (whole.len() - 1) / BLOCK_SIZE;
		assert!(last_block >= 3);

		for mode in MODES {
			let dir = work_copy(template.path());
			let mut bytes = whole.clone();
			bytes[last_block * BLOCK_SIZE..].fill(0);
			write(dir.path(), 0, &bytes);
			let before = snapshot(dir.path());
			// The tail that was zeroed is not data that is there: the log ends where the zeros
			// begin, and the batch that did not end is not recovered.
			let recovered = recover(&context, dir.path(), mode).unwrap();
			recovered.assert_keys(&all, &["p0-key-0000"], &format!("{suite:?}, {mode:?}"));
			assert_eq!(snapshot(dir.path()), before, "{suite:?}, {mode:?}");
		}
	}
}

#[test]
fn a_header_that_was_cut_short_is_a_tear_and_a_segment_with_nothing_in_it_is_not() {
	for suite in SUITES {
		let mut plan = standard_plan();
		plan.push(Vec::new());
		let template = TempDir::new().unwrap();
		let enc = context(suite);
		build(template.path(), &enc, CompressionType::None, &plan);
		let context = sealed_context(&enc);
		let whole = read(template.path(), 3);
		assert_eq!(whole.len(), HEADER_RECORD_LEN);

		// Empty, or whole: nothing was lost, whatever the mode.
		for (cut, what) in [(0, "empty"), (HEADER_RECORD_LEN, "whole")] {
			for mode in MODES {
				let dir = work_copy(template.path());
				write(dir.path(), 3, &whole[..cut]);
				let before = snapshot(dir.path());
				let recovered = recover(&context, dir.path(), mode).unwrap();
				recovered.assert_keys(&KEYS, &KEYS, &format!("{suite:?}, {what}, {mode:?}"));
				assert_eq!(snapshot(dir.path()), before, "{suite:?}, {what}, {mode:?}");
			}
		}

		// Cut inside the header: damage at the end of the log, which the default repairs.
		for cut in [1, 6, 7, 50, HEADER_RECORD_LEN - 1] {
			let what = format!("{suite:?}, the header cut at {cut}");
			let absolute = work_copy(template.path());
			write(absolute.path(), 3, &whole[..cut]);
			let before = snapshot(absolute.path());
			let error = refused(&context, absolute.path(), WalRecoveryMode::AbsoluteConsistency);
			assert!(
				matches!(
					error,
					Error::WalCorruption {
						segment_id: 3,
						offset: 0,
						..
					}
				),
				"{what}: {error}"
			);
			assert_eq!(snapshot(absolute.path()), before, "{what}");

			let dir = work_copy(template.path());
			write(dir.path(), 3, &whole[..cut]);
			let recovered =
				recover(&context, dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair)
					.unwrap();
			recovered.assert_keys(&KEYS, &KEYS, &what);
			assert_eq!(
				fs::read(dir.path().join(format!("{}.corrupt", segment_name(3, "wal")))).unwrap(),
				&whole[..cut]
			);
			assert_eq!(read(dir.path(), 3).len(), HEADER_RECORD_LEN, "{what}");
			assert!(read_records(&context, &segment_path(dir.path(), 3), 3).unwrap().is_empty());
		}
	}
}

// ---------------------------------------------------------------------------
// parallel recovery
// ---------------------------------------------------------------------------

#[test]
fn segments_decoded_in_parallel_are_the_segments_decoded_one_at_a_time() {
	for suite in SUITES {
		for compression in [CompressionType::None, CompressionType::Lz4] {
			let plan = standard_plan();
			let template = TempDir::new().unwrap();
			let enc = context(suite);
			build(template.path(), &enc, compression, &plan);
			let context = sealed_context(&enc);

			let segments: Vec<SegmentRef> = (0..3u64)
				.map(|id| SegmentRef {
					file_path: segment_path(template.path(), id),
					id,
				})
				.collect();
			let together = decode_sealed_segments(&context, &segments).unwrap();
			assert_eq!(together.len(), 3);
			for (segment, decoded) in segments.iter().zip(&together) {
				let alone = decode_segment(&context, &segment.file_path, segment.id).unwrap();
				assert_eq!(decoded.id, alone.id);
				assert_eq!(decoded.max_seq, alone.max_seq);
				assert_eq!(decoded.end, SegmentEnd::Clean);
				assert_eq!(alone.end, SegmentEnd::Clean);
				let encoded = |batches: &[crate::batch::Batch]| {
					batches.iter().map(|batch| batch.encode().unwrap()).collect::<Vec<_>>()
				};
				assert_eq!(encoded(&decoded.batches), encoded(&alone.batches));
				assert_eq!(encoded(&decoded.batches), plan[segment.id as usize]);
			}

			// A worker that finds a key that does not fit fails the whole decode with that error,
			// and not with a tear: the workers have the context they were given.
			let wrong = sealed_context(&context_of([0x99; 32], DB, suite));
			let error = decode_sealed_segments(&wrong, &segments).unwrap_err();
			assert!(is_encryption(&error, &EncryptionError::WrongKey), "{error}");
		}
	}
}

#[test]
fn a_wal_of_one_segment_and_a_wal_of_many_are_recovered_alike() {
	for suite in SUITES {
		let enc = context(suite);
		let context = sealed_context(&enc);
		for segments in 1..=3usize {
			let plan: Plan = standard_plan().into_iter().take(segments).collect();
			let dir = TempDir::new().unwrap();
			build(dir.path(), &enc, CompressionType::None, &plan);
			let before = snapshot(dir.path());
			let recovered =
				recover(&context, dir.path(), WalRecoveryMode::AbsoluteConsistency).unwrap();
			let present: Vec<&str> = KEYS
				.iter()
				.copied()
				.filter(|key| ["a", "b", "c"][..segments].iter().any(|tag| key.starts_with(tag)))
				.collect();
			recovered.assert_keys(&KEYS, &present, &format!("{suite:?}, {segments} segments"));
			assert_eq!(recovered.memtables.len(), segments);
			assert_eq!(snapshot(dir.path()), before, "recovery only reads");
		}
	}
}
