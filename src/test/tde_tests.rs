//! Property and concurrency tests of the encryption building blocks.
//!
//! The data is kept small: AEAD runs at a few MB/s in the debug profile, and these tests are
//! about what is bound to a block, not about throughput.

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use proptest::prelude::*;

use crate::error::EncryptionError;
use crate::tde::{
	BlockContext,
	BlockKind,
	CipherSuite,
	CryptoHeader,
	DbId,
	EncryptionContext,
	FileCipher,
	FileIdentity,
	FileKey,
	FileKind,
	SoftwareKeyManager,
	HEADER_LEN,
	TAG_LEN,
};

fn config() -> ProptestConfig {
	ProptestConfig {
		cases: 64,
		// Do not write a regression file next to the sources when a case fails in CI.
		failure_persistence: None,
		..ProptestConfig::default()
	}
}

const ALL_BLOCK_KINDS: [BlockKind; 9] = [
	BlockKind::SstData,
	BlockKind::SstIndexPartition,
	BlockKind::SstTopLevelIndex,
	BlockKind::SstFilterPartition,
	BlockKind::SstFilterIndex,
	BlockKind::SstMetaIndex,
	BlockKind::VlogEntry,
	BlockKind::WalBatch,
	BlockKind::ManifestBody,
];

const ALL_FILE_KINDS: [FileKind; 4] =
	[FileKind::Sst, FileKind::Vlog, FileKind::Wal, FileKind::Manifest];

fn arb_suite() -> impl Strategy<Value = CipherSuite> {
	prop_oneof![Just(CipherSuite::Aes256Gcm), Just(CipherSuite::XChaCha20Poly1305)]
}

fn arb_salt() -> impl Strategy<Value = [u8; 32]> {
	any::<[u8; 32]>().prop_filter("a salt is never all zero", |salt| salt.iter().any(|&b| b != 0))
}

/// One block of one file: the file's key material and identity, and the block's context and
/// plaintext.
#[derive(Debug, Clone)]
struct Case {
	root: [u8; 32],
	salt: [u8; 32],
	identity: FileIdentity,
	block_kind: BlockKind,
	position: u64,
	compression: u8,
	plaintext: Vec<u8>,
}

impl Case {
	fn context(&self) -> BlockContext {
		BlockContext::new(self.block_kind, self.position, self.compression)
	}

	fn cipher_for(
		root: &[u8; 32],
		salt: [u8; 32],
		identity: FileIdentity,
		can_seal: bool,
	) -> FileCipher {
		FileCipher::new(FileKey::derive_for_test(root, salt, identity, can_seal)).unwrap()
	}

	fn writer(&self) -> FileCipher {
		Self::cipher_for(&self.root, self.salt, self.identity, true)
	}

	fn reader(&self) -> FileCipher {
		Self::cipher_for(&self.root, self.salt, self.identity, false)
	}

	fn sealed(&self) -> Vec<u8> {
		let mut buf = self.plaintext.clone();
		self.writer().seal_append(&self.context(), &mut buf).unwrap();
		buf
	}
}

prop_compose! {
	/// A block of any kind in a file of the matching kind, of any suite, id, key id and database.
	fn arb_case()(
		block_kind in prop::sample::select(ALL_BLOCK_KINDS.to_vec()),
		root in any::<[u8; 32]>(),
		salt in arb_salt(),
		suite in arb_suite(),
		key_id in any::<u32>(),
		file_id in any::<u64>(),
		db in any::<[u8; 16]>(),
		// Half the range, so that a position can always be moved up without overflowing.
		position in 0..(u64::MAX / 2),
		compression in any::<u8>(),
		plaintext in prop::collection::vec(any::<u8>(), 0..=192),
	) -> Case {
		Case {
			root,
			salt,
			identity: FileIdentity::new(
				suite,
				block_kind.file_kind(),
				key_id,
				file_id,
				DbId::from_bytes(db),
			),
			block_kind,
			position,
			compression,
			plaintext,
		}
	}
}

proptest! {
	#![proptest_config(config())]

	#[test]
	fn a_sealed_block_opens_to_what_was_sealed(case in arb_case()) {
		let mut sealed = case.sealed();
		prop_assert_eq!(sealed.len(), case.plaintext.len() + TAG_LEN);
		let opened = case.reader().open_suffix(&case.context(), &mut sealed).unwrap();
		prop_assert_eq!(&opened[..], &case.plaintext[..]);
	}

	#[test]
	fn the_detached_form_is_the_appended_form_split_in_two(case in arb_case()) {
		let appended = case.sealed();
		let mut data = case.plaintext.clone();
		let tag = case.writer().seal_detached(&case.context(), &mut data).unwrap();
		let mut joined = data.clone();
		joined.extend_from_slice(&tag);
		prop_assert_eq!(joined, appended);
		case.reader().open_detached(&case.context(), &mut data, &tag).unwrap();
		prop_assert_eq!(data, case.plaintext);
	}

	#[test]
	fn flipping_any_bit_of_a_sealed_block_fails_authentication(
		case in arb_case(),
		at in any::<prop::sample::Index>(),
		bit in 0u8..8,
	) {
		let mut sealed = case.sealed();
		let i = at.index(sealed.len());
		sealed[i] ^= 1 << bit;
		let before = sealed.clone();
		let err = case.reader().open_suffix(&case.context(), &mut sealed).unwrap_err();
		prop_assert_eq!(err, EncryptionError::AuthenticationFailed);
		// A rejected block is left as it was found, so nothing unauthenticated is exposed.
		prop_assert_eq!(sealed, before);
	}

	#[test]
	fn changing_anything_a_block_is_bound_to_fails_authentication(
		case in arb_case(),
		which in 0u8..10,
		// Which single bit of the chosen field is flipped, taken modulo the width of the field.
		bit in 0u32..128,
	) {
		// The block-kind change below stays within SSTable blocks, which are the file kind that
		// has several.
		prop_assume!(case.block_kind.file_kind() == FileKind::Sst);
		let mut root = case.root;
		let mut salt = case.salt;
		let mut identity = case.identity;
		let mut context = case.context();
		match which {
			0 => {
				let position = case.position ^ (1u64 << (bit % 64));
				context = BlockContext::new(case.block_kind, position, case.compression);
			}
			1 => {
				let other = if case.block_kind == BlockKind::SstData {
					BlockKind::SstMetaIndex
				} else {
					BlockKind::SstData
				};
				context = BlockContext::new(other, case.position, case.compression);
			}
			2 => {
				let compression = case.compression ^ (1u8 << (bit % 8));
				context = BlockContext::new(case.block_kind, case.position, compression);
			}
			3 => identity.file_id ^= 1u64 << (bit % 64),
			4 => identity.key_id ^= 1u32 << (bit % 32),
			5 => {
				let mut db = *identity.db_id.as_bytes();
				db[(bit / 8) as usize] ^= 1u8 << (bit % 8);
				identity.db_id = DbId::from_bytes(db);
			}
			6 => {
				identity.suite = if identity.suite == CipherSuite::Aes256Gcm {
					CipherSuite::XChaCha20Poly1305
				} else {
					CipherSuite::Aes256Gcm
				};
			}
			7 => salt[(bit / 8) as usize] ^= 1u8 << (bit % 8),
			8 => root[(bit / 8) as usize] ^= 1u8 << (bit % 8),
			_ => {
				// Read as a vlog entry of another file kind.
				identity.file_kind = FileKind::Vlog;
				context = BlockContext::new(BlockKind::VlogEntry, case.position, case.compression);
			}
		}
		let reader = Case::cipher_for(&root, salt, identity, false);
		let mut sealed = case.sealed();
		let err = reader.open_suffix(&context, &mut sealed).unwrap_err();
		prop_assert_eq!(err, EncryptionError::AuthenticationFailed, "change {}", which);
	}

	#[test]
	fn a_block_cannot_be_moved_to_another_file_or_position_of_the_same_database(
		file_kind in prop::sample::select(ALL_FILE_KINDS.to_vec()),
		suite in arb_suite(),
		file_id in any::<u64>(),
		position in 0..(u64::MAX / 2),
		plaintext in prop::collection::vec(any::<u8>(), 0..=96),
	) {
		let km = Arc::new(SoftwareKeyManager::new([0x2A; 32]));
		let ctx = EncryptionContext::new(km, DbId::from_bytes([0x77; 16]), suite);
		let block_kind = match file_kind {
			FileKind::Sst => BlockKind::SstData,
			FileKind::Vlog => BlockKind::VlogEntry,
			FileKind::Wal => BlockKind::WalBatch,
			FileKind::Manifest => BlockKind::ManifestBody,
		};
		let context = BlockContext::new(block_kind, position, 0);

		// Two files of the same kind and id, written by two writers: only their salts differ.
		let (header_a, writer_a) = ctx.create_file(file_kind, file_id, b"").unwrap();
		let (header_b, _) = ctx.create_file(file_kind, file_id, b"").unwrap();
		let reader_a = ctx.open_file(&header_a, file_kind, file_id, b"").unwrap();
		let reader_b = ctx.open_file(&header_b, file_kind, file_id, b"").unwrap();

		let mut sealed = plaintext.clone();
		writer_a.seal_append(&context, &mut sealed).unwrap();

		// Control: its own file, its own position.
		let mut own = sealed.clone();
		prop_assert_eq!(reader_a.open_suffix(&context, &mut own).unwrap(), &plaintext[..]);
		// Another writer of the same file identity.
		let mut moved = sealed.clone();
		prop_assert_eq!(
			reader_b.open_suffix(&context, &mut moved).unwrap_err(),
			EncryptionError::AuthenticationFailed
		);
		// The same file at another offset.
		let mut shifted = sealed;
		let elsewhere = BlockContext::new(block_kind, position + 1, 0);
		prop_assert_eq!(
			reader_a.open_suffix(&elsewhere, &mut shifted).unwrap_err(),
			EncryptionError::AuthenticationFailed
		);
	}

	#[test]
	fn two_positions_never_seal_to_the_same_bytes(case in arb_case(), step in 1u64..1000) {
		let moved = Case { position: case.position + step, ..case.clone() };
		prop_assert_ne!(case.sealed(), moved.sealed());
	}

	#[test]
	fn arbitrary_bytes_never_open(
		case in arb_case(),
		junk in prop::collection::vec(any::<u8>(), 0..300),
	) {
		let mut junk = junk;
		let err = case.reader().open_suffix(&case.context(), &mut junk).unwrap_err();
		prop_assert_eq!(err, EncryptionError::AuthenticationFailed);
	}

	#[test]
	fn a_header_survives_encoding_and_decoding(
		suite in arb_suite(),
		file_kind in prop::sample::select(ALL_FILE_KINDS.to_vec()),
		key_id in any::<u32>(),
		file_id in any::<u64>(),
		db in any::<[u8; 16]>(),
		salt in arb_salt(),
		tag in any::<[u8; 16]>(),
	) {
		let header = CryptoHeader {
			identity: FileIdentity::new(suite, file_kind, key_id, file_id, DbId::from_bytes(db)),
			salt,
			tag,
		};
		let bytes = header.encode();
		prop_assert_eq!(bytes.len(), HEADER_LEN);
		prop_assert_eq!(CryptoHeader::decode(&bytes).unwrap(), header);
	}

	#[test]
	fn the_header_decoder_is_total_and_only_accepts_the_canonical_encoding(
		junk in prop::collection::vec(any::<u8>(), 0..200),
	) {
		// Whatever the bytes, decoding returns instead of panicking, and anything it accepts is
		// exactly what encoding that header would produce.
		if let Ok(header) = CryptoHeader::decode(&junk) {
			let encoded = header.encode();
			prop_assert_eq!(&encoded[..], &junk[..HEADER_LEN]);
		}
	}

	#[test]
	fn a_damaged_header_with_a_fixed_crc_is_refused_or_canonical(
		suite in arb_suite(),
		file_kind in prop::sample::select(ALL_FILE_KINDS.to_vec()),
		salt in arb_salt(),
		damage in prop::collection::vec((0usize..HEADER_LEN - 4, any::<u8>()), 1..4),
	) {
		let header = CryptoHeader {
			identity: FileIdentity::new(suite, file_kind, 1, 2, DbId::from_bytes([3; 16])),
			salt,
			tag: [4; 16],
		};
		let mut bytes = header.encode();
		for (at, value) in damage {
			bytes[at] = value;
		}
		let crc = crc32fast::hash(&bytes[..HEADER_LEN - 4]);
		bytes[HEADER_LEN - 4..].copy_from_slice(&crc.to_be_bytes());
		// Bytes the decoder does not accept are refused; the rest decode to a header that encodes
		// back to exactly these bytes, so no byte is read loosely.
		if let Ok(decoded) = CryptoHeader::decode(&bytes) {
			let encoded = decoded.encode();
			prop_assert_eq!(&encoded[..], &bytes[..]);
		}
	}
}

fn writer_and_reader(suite: CipherSuite) -> (Arc<FileCipher>, Arc<FileCipher>) {
	let identity = FileIdentity::new(suite, FileKind::Wal, 1, 1, DbId::from_bytes([9; 16]));
	let root = [0x31; 32];
	let salt = [0x32; 32];
	(
		Arc::new(Case::cipher_for(&root, salt, identity, true)),
		Arc::new(Case::cipher_for(&root, salt, identity, false)),
	)
}

fn wal_context(position: u64) -> BlockContext {
	BlockContext::new(BlockKind::WalBatch, position, 0)
}

#[test]
fn of_several_threads_sealing_one_position_exactly_one_succeeds() {
	for suite in [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305] {
		let (writer, _) = writer_and_reader(suite);
		let results: Vec<_> = thread::scope(|scope| {
			let handles: Vec<_> = (0..8)
				.map(|_| {
					let writer = Arc::clone(&writer);
					scope.spawn(move || writer.seal_append(&wal_context(7), &mut b"batch".to_vec()))
				})
				.collect();
			handles.into_iter().map(|handle| handle.join().unwrap()).collect()
		});
		let successes = results.iter().filter(|result| result.is_ok()).count();
		assert_eq!(successes, 1, "{suite:?}: {results:?}");
		for result in results.iter().filter(|result| result.is_err()) {
			assert_eq!(result, &Err(EncryptionError::PositionReuse), "{suite:?}");
		}
		assert_eq!(writer.next_position(), 8);
	}
}

#[test]
fn threads_racing_through_the_same_positions_never_seal_one_twice() {
	for suite in [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305] {
		let (writer, reader) = writer_and_reader(suite);
		let sealed: Vec<(u64, Vec<u8>)> = thread::scope(|scope| {
			let handles: Vec<_> = (0..4)
				.map(|thread_id| {
					let writer = Arc::clone(&writer);
					scope.spawn(move || {
						let mut mine = Vec::new();
						for position in 0..40u64 {
							let mut block = format!("thread {thread_id}").into_bytes();
							match writer.seal_append(&wal_context(position), &mut block) {
								Ok(()) => mine.push((position, block)),
								Err(err) => assert_eq!(err, EncryptionError::PositionReuse),
							}
						}
						mine
					})
				})
				.collect();
			handles.into_iter().flat_map(|handle| handle.join().unwrap()).collect()
		});

		// Whichever thread got there first, no position was sealed twice.
		let mut per_position: HashMap<u64, usize> = HashMap::new();
		for (position, _) in &sealed {
			*per_position.entry(*position).or_default() += 1;
		}
		assert!(!sealed.is_empty(), "{suite:?}: nothing was sealed");
		assert!(per_position.values().all(|&count| count == 1), "{suite:?}: {per_position:?}");
		// Every block that was sealed is authentic, and says which thread sealed it.
		for (position, mut block) in sealed {
			let opened = reader.open_suffix(&wal_context(position), &mut block).unwrap();
			assert!(opened.starts_with(b"thread "), "{suite:?}");
		}
	}
}

#[test]
fn a_shared_cipher_opens_blocks_from_many_threads() {
	for suite in [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305] {
		let (writer, reader) = writer_and_reader(suite);
		let blocks: Vec<(u64, Vec<u8>)> = (1..=24u64)
			.map(|position| {
				let mut block = format!("record {position}").into_bytes();
				writer.seal_append(&wal_context(position), &mut block).unwrap();
				(position, block)
			})
			.collect();
		let blocks = Arc::new(blocks);

		thread::scope(|scope| {
			for _ in 0..8 {
				let reader = Arc::clone(&reader);
				let blocks = Arc::clone(&blocks);
				scope.spawn(move || {
					for (position, sealed) in blocks.iter() {
						let mut copy = sealed.clone();
						let opened =
							reader.open_suffix(&wal_context(*position), &mut copy).unwrap();
						assert_eq!(opened, format!("record {position}").as_bytes());
					}
				});
			}
		});
	}
}
