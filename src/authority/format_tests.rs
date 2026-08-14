use super::format::*;
use crate::{BranchGeneration, BranchId};

fn sample_catalog() -> CatalogManifest {
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
			CatalogEntry {
				branch: BranchId::from_u128(10),
				name: "agent/expired".to_owned(),
				generation: BranchGeneration(2),
				status: BranchStatus::Deleted,
				created_at_seq: 17,
				parent: None,
				deleted_at_seq: Some(30),
				expires_at: None,
			},
		],
	}
}

fn sample_state() -> BranchStateManifest {
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

fn sample_root() -> RootManifest {
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
fn catalog_round_trips() {
	let manifest = sample_catalog();
	let encoded = manifest.encode().unwrap();
	assert_eq!(CatalogManifest::decode(&encoded).unwrap(), manifest);
}

#[test]
fn state_round_trips() {
	let manifest = sample_state();
	let encoded = manifest.encode().unwrap();
	assert_eq!(BranchStateManifest::decode(&encoded).unwrap(), manifest);
}

#[test]
fn root_round_trips() {
	let manifest = sample_root();
	let encoded = manifest.encode().unwrap();
	assert_eq!(RootManifest::decode(&encoded).unwrap(), manifest);
}

/// The fail-closed decode ladder, exercised per format: bad magic, pre-v1,
/// future version, checksum mismatch, trailing bytes. Each arm starts from a
/// valid encoding (non-vacuity) and plants exactly one defect.
#[test]
fn decode_ladder_fails_closed_per_format() {
	let cases: Vec<(&str, Vec<u8>)> = vec![
		("catalog manifest", sample_catalog().encode().unwrap()),
		("branch state manifest", sample_state().encode().unwrap()),
		("root manifest", sample_root().encode().unwrap()),
	];
	for (format, valid) in cases {
		let decode = |bytes: &[u8]| -> String {
			let error = match format {
				"catalog manifest" => CatalogManifest::decode(bytes).err(),
				"branch state manifest" => BranchStateManifest::decode(bytes).err(),
				_ => RootManifest::decode(bytes).err(),
			};
			error.expect("planted defect must fail decode").to_string()
		};

		let mut bad_magic = valid.clone();
		bad_magic[0] ^= 0xFF;
		assert!(decode(&bad_magic).contains("bad magic"), "{format}");

		let mut pre_v1 = valid.clone();
		pre_v1[4..6].copy_from_slice(&0u16.to_le_bytes());
		assert!(decode(&pre_v1).contains("pre-v1"), "{format}");

		let mut future = valid.clone();
		future[4..6].copy_from_slice(&2u16.to_le_bytes());
		assert!(decode(&future).contains("future format version 2"), "{format}");

		let mut corrupt = valid.clone();
		let flip = valid.len() / 2;
		corrupt[flip] ^= 0x01;
		assert!(decode(&corrupt).contains("checksum mismatch"), "{format}");

		// Trailing bytes after a fully valid body + refreshed checksum.
		let mut trailing = valid[..valid.len() - 4].to_vec();
		trailing.push(0xAB);
		let crc = crc32fast::hash(&trailing);
		trailing.extend_from_slice(&crc.to_le_bytes());
		assert!(decode(&trailing).contains("trailing bytes"), "{format}");
	}
}

#[test]
fn catalog_rejects_unsorted_entries_and_bad_status_pairs() {
	let mut manifest = sample_catalog();
	manifest.entries.swap(0, 1);
	let error = manifest.encode().expect_err("unsorted entries must fail").to_string();
	assert!(error.contains("strictly ascending"), "{error}");

	let mut manifest = sample_catalog();
	manifest.entries[2].deleted_at_seq = None;
	let error = manifest.encode().expect_err("deleted without anchor must fail").to_string();
	assert!(error.contains("deleted_at_seq"), "{error}");

	let mut manifest = sample_catalog();
	manifest.entries[1].generation = BranchGeneration(5);
	let error = manifest.encode().expect_err("generation at watermark must fail").to_string();
	assert!(error.contains("allocator watermark"), "{error}");
}

#[test]
fn catalog_rejects_entry_count_beyond_cap_before_allocation() {
	// Craft a header claiming an enormous entry count with no entry bytes:
	// the cap must reject it before any allocation is attempted.
	let mut bytes = Vec::new();
	bytes.extend_from_slice(&CATALOG_MAGIC);
	bytes.extend_from_slice(&AUTHORITY_FORMAT_VERSION.to_le_bytes());
	bytes.extend_from_slice(&[0x20; 16]);
	bytes.extend_from_slice(&1u64.to_le_bytes());
	bytes.extend_from_slice(&1u64.to_le_bytes());
	bytes.extend_from_slice(&0u64.to_le_bytes());
	bytes.extend_from_slice(&0u64.to_le_bytes());
	bytes.extend_from_slice(&(u32::MAX).to_le_bytes());
	let crc = crc32fast::hash(&bytes);
	bytes.extend_from_slice(&crc.to_le_bytes());
	let error = CatalogManifest::decode(&bytes).expect_err("cap must reject").to_string();
	assert!(error.contains("exceeds cap"), "{error}");
}

#[test]
fn state_rejects_duplicate_table_ids_across_levels() {
	let mut manifest = sample_state();
	manifest.levels[2] = vec![7];
	let error = manifest.encode().expect_err("duplicate table id must fail").to_string();
	assert!(error.contains("listed more than once"), "{error}");
}

#[test]
fn root_rejects_non_monotone_timeline_and_unsorted_hints() {
	let mut manifest = sample_root();
	manifest.timeline_tail = vec![(2_000, 250), (1_000, 100)];
	let error = manifest.encode().expect_err("regressing timeline must fail").to_string();
	assert!(error.contains("strictly increase"), "{error}");

	let mut manifest = sample_root();
	manifest.state_hints.swap(0, 1);
	let error = manifest.encode().expect_err("unsorted hints must fail").to_string();
	assert!(error.contains("strictly ascending"), "{error}");
}
