//! Golden vectors: each format's encoding is byte-pinned bidirectionally
//! (encode(value) == golden AND decode(golden) == value), so any layout
//! drift — field order, widths, endianness, checksum placement — fails
//! these tests rather than silently changing the durable format.
//!
//! Do not regenerate these constants to make a failing test pass: a mismatch
//! means the durable format changed, which requires a format-version bump and
//! a new vector alongside the old one's removal — a reviewed decision.

use super::format::*;
use crate::{BranchGeneration, BranchId};

fn unhex(hex: &str) -> Vec<u8> {
	let clean: String = hex.chars().filter(|c| c.is_ascii_hexdigit()).collect();
	clean
		.as_bytes()
		.chunks(2)
		.map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
		.collect()
}

fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// SKBC v1: db_id 0x20×16, catalog_version 7, next_generation 5,
/// writer_epoch 3, maintenance_epoch 2; entries: main (default id, gen 0,
/// active, created 0, no options) and agent/sandbox (id 9, gen 4, active,
/// created 41, parent=default@0 fork_seq 41, expires 99000).
const GOLDEN_CATALOG: &str = "534b42430100202020202020202020202020202020200700000000000000050000000000000003000000000000000200000000000000020000000000000000000000000000000000000004006d61696e000000000000000000000000000000000000000000000000000000000000000000090d006167656e742f73616e64626f780400000000000000000529000000000000000000000000000000000000000000000000000000000000002900000000000000b88201000000000046d7cb17";

/// SKBM v1: branch id 9, generation 4, state_version 3, last_sequence 120,
/// flushed_log_number 2, retained_floor_seq 64; levels [ [7,5], [], [9] ].
const GOLDEN_STATE: &str = "534b424d01000000000000000000000000000000000904000000000000000300000000000000780000000000000002000000000000004000000000000000030200000007000000000000000500000000000000000000000100000009000000000000001b049148";

/// SKRT v1: db_id 0x20×16, root_version 11, visible_seq 400,
/// last_commit_ts 9000, timeline [(1000,100),(2000,250)],
/// wal_reclaim_floor 6, next_table_id 2048, catalog_version_floor 4,
/// hints [(default@0, v8), (id 9@4, v3)].
const GOLDEN_ROOT: &str = "534b52540100202020202020202020202020202020200b00000000000000900100000000000028230000000000000200e8030000000000006400000000000000d007000000000000fa000000000000000600000000000000000800000000000004000000000000000200000000000000000000000000000000000000000000000000000008000000000000000000000000000000000000000000000904000000000000000300000000000000e3d64ab6";

fn golden_catalog_value() -> CatalogManifest {
	CatalogManifest {
		db_id: [0x20; 16],
		catalog_version: 7,
		next_generation: 5,
		writer_epoch: 3,
		maintenance_epoch: 2,
		entries: vec![
			CatalogEntry {
				branch: BranchId::DEFAULT,
				name: "main".to_owned(),
				generation: BranchGeneration(0),
				status: BranchStatus::Active,
				created_at_seq: 0,
				parent: None,
				deleted_at_seq: None,
				expires_at: None,
			},
			CatalogEntry {
				branch: BranchId::from_u128(9),
				name: "agent/sandbox".to_owned(),
				generation: BranchGeneration(4),
				status: BranchStatus::Active,
				created_at_seq: 41,
				parent: Some(ParentLink {
					parent: BranchId::DEFAULT,
					parent_generation: BranchGeneration(0),
					fork_seq: 41,
				}),
				deleted_at_seq: None,
				expires_at: Some(99_000),
			},
		],
	}
}

fn golden_state_value() -> BranchStateManifest {
	BranchStateManifest {
		branch: BranchId::from_u128(9),
		generation: BranchGeneration(4),
		state_version: 3,
		last_sequence: 120,
		flushed_log_number: 2,
		retained_floor_seq: 64,
		levels: vec![vec![7, 5], vec![], vec![9]],
	}
}

fn golden_root_value() -> RootManifest {
	RootManifest {
		db_id: [0x20; 16],
		root_version: 11,
		visible_seq: 400,
		last_commit_ts: 9_000,
		timeline_tail: vec![(1_000, 100), (2_000, 250)],
		wal_reclaim_floor: 6,
		next_table_id: 2_048,
		catalog_version_floor: 4,
		state_hints: vec![
			(BranchId::DEFAULT, BranchGeneration(0), 8),
			(BranchId::from_u128(9), BranchGeneration(4), 3),
		],
	}
}

#[test]
fn catalog_golden_is_byte_pinned_bidirectionally() {
	let value = golden_catalog_value();
	assert_eq!(hex(&value.encode().unwrap()), GOLDEN_CATALOG, "encode drifted from golden");
	assert_eq!(
		CatalogManifest::decode(&unhex(GOLDEN_CATALOG)).unwrap(),
		value,
		"decode(golden) drifted from the pinned value"
	);
}

#[test]
fn state_golden_is_byte_pinned_bidirectionally() {
	let value = golden_state_value();
	assert_eq!(hex(&value.encode().unwrap()), GOLDEN_STATE, "encode drifted from golden");
	assert_eq!(
		BranchStateManifest::decode(&unhex(GOLDEN_STATE)).unwrap(),
		value,
		"decode(golden) drifted from the pinned value"
	);
}

#[test]
fn root_golden_is_byte_pinned_bidirectionally() {
	let value = golden_root_value();
	assert_eq!(hex(&value.encode().unwrap()), GOLDEN_ROOT, "encode drifted from golden");
	assert_eq!(
		RootManifest::decode(&unhex(GOLDEN_ROOT)).unwrap(),
		value,
		"decode(golden) drifted from the pinned value"
	);
}

/// Sabotage twin for the golden harness itself: a single flipped byte in a
/// golden must redden the bidirectional check (the vectors are not vacuous).
#[test]
fn golden_harness_detects_a_single_byte_flip() {
	let mut flipped = unhex(GOLDEN_STATE);
	let index = flipped.len() / 2;
	flipped[index] ^= 0x01;
	assert!(
		BranchStateManifest::decode(&flipped).is_err(),
		"a flipped golden byte must fail decode (checksum)"
	);
}
