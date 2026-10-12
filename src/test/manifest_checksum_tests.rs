use std::fs;
use std::io::{Cursor, Read};
use std::sync::Arc;

use crate::levels::{
	DecodedManifest,
	SnapshotInfo,
	MANIFEST_FORMAT_VERSION_V1,
	MANIFEST_TRAILER_LEN,
	MANIFEST_TRAILER_MAGIC,
};
use crate::{Error, Options, Tree};

/// Reference implementation of the pre-trailer v0.21.4 manifest decoder.
/// Verifies that older versions can read a manifest written with the new trailer.
fn decode_legacy_manifest_v0_21_4(data: &[u8]) -> crate::Result<DecodedManifest> {
	let mut cursor = Cursor::new(data);

	let mut u16_buf = [0u8; 2];
	cursor.read_exact(&mut u16_buf)?;
	let version = u16::from_be_bytes(u16_buf);
	if version != MANIFEST_FORMAT_VERSION_V1 {
		return Err(Error::LoadManifestFail(format!(
			"Unsupported manifest format version: {version}"
		)));
	}

	let mut u64_buf = [0u8; 8];
	cursor.read_exact(&mut u64_buf)?;
	let next_table_id = u64::from_be_bytes(u64_buf);

	cursor.read_exact(&mut u64_buf)?;
	let log_number = u64::from_be_bytes(u64_buf);

	cursor.read_exact(&mut u64_buf)?;
	let last_sequence = u64::from_be_bytes(u64_buf);

	let level_data = crate::levels::Levels::decode(&mut cursor)?;

	let mut u32_buf = [0u8; 4];
	cursor.read_exact(&mut u32_buf)?;
	let snapshot_count = u32::from_be_bytes(u32_buf);
	let mut snapshots = Vec::new();
	for _ in 0..snapshot_count {
		cursor.read_exact(&mut u32_buf)?;
		let snapshot_len = u32::from_be_bytes(u32_buf) as usize;
		let mut snapshot_bytes = vec![0u8; snapshot_len];
		cursor.read_exact(&mut snapshot_bytes)?;
		let snapshot = SnapshotInfo::decode(&snapshot_bytes)?;
		snapshots.push(snapshot);
	}

	Ok(DecodedManifest {
		version,
		next_table_id,
		log_number,
		last_sequence,
		level_data,
		snapshots,
	})
}

async fn set_key(tree: &Tree, key: &[u8], val: &[u8]) {
	let mut txn = tree.begin().expect("begin txn");
	txn.set(key, val).expect("set");
	txn.commit().await.expect("commit");
}

fn get_key(tree: &Tree, key: &[u8]) -> Option<Vec<u8>> {
	let txn = tree.begin().expect("begin txn");
	txn.get(key).expect("get")
}

#[tokio::test]
async fn test_manifest_checksum_written_and_verified() {
	let temp_dir = tempfile::tempdir().expect("temp dir");
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let tree = Tree::new(Arc::clone(&opts)).expect("open tree");
	set_key(&tree, b"key1", b"val1").await;
	tree.flush().expect("flush");
	tree.close().await.expect("close");

	let manifest_path = opts.manifest_file_path(0);
	assert!(manifest_path.exists());
	let raw_bytes = fs::read(&manifest_path).expect("read manifest");

	assert!(raw_bytes.len() >= 26 + MANIFEST_TRAILER_LEN);
	let magic = &raw_bytes[raw_bytes.len() - 8..];
	assert_eq!(magic, &MANIFEST_TRAILER_MAGIC);

	let decoded = DecodedManifest::decode(raw_bytes.clone()).expect("decode manifest");
	assert_eq!(decoded.version, MANIFEST_FORMAT_VERSION_V1);

	// Reopen succeeds with validated trailer
	let tree2 = Tree::new(Arc::clone(&opts)).expect("reopen tree");
	let val = get_key(&tree2, b"key1");
	assert_eq!(val, Some(b"val1".to_vec()));
	tree2.close().await.expect("close tree2");
}

#[tokio::test]
async fn test_single_byte_bit_flips_refused() {
	let temp_dir = tempfile::tempdir().expect("temp dir");
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let tree = Tree::new(Arc::clone(&opts)).expect("open tree");
	set_key(&tree, b"key1", b"val1").await;
	tree.flush().expect("flush");
	tree.close().await.expect("close");

	let manifest_path = opts.manifest_file_path(0);
	let original = fs::read(&manifest_path).expect("read manifest");
	assert!(original.len() > MANIFEST_TRAILER_LEN);

	// Test flipping a bit at every single byte position in the file.
	// Every single flip must be refused by DecodedManifest::decode.
	for i in 0..original.len() {
		let mut corrupted = original.clone();
		corrupted[i] ^= 0x01;

		let result = DecodedManifest::decode(corrupted);
		assert!(
			result.is_err(),
			"Bit flip at offset {i} of {} was unexpectedly accepted",
			original.len()
		);
	}
}

#[tokio::test]
async fn test_legacy_manifest_without_trailer_accepted_and_upgraded_on_write() {
	let temp_dir = tempfile::tempdir().expect("temp dir");
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let tree = Tree::new(Arc::clone(&opts)).expect("open tree");
	set_key(&tree, b"key_legacy", b"val_legacy").await;
	tree.flush().expect("flush");
	tree.close().await.expect("close");

	let manifest_path = opts.manifest_file_path(0);
	let original = fs::read(&manifest_path).expect("read manifest");

	// Strip the trailer to create a legacy format manifest
	let legacy_bytes = original[..original.len() - MANIFEST_TRAILER_LEN].to_vec();

	// Verify decode accepts legacy bytes
	let decoded = DecodedManifest::decode(legacy_bytes.clone()).expect("legacy decode");
	assert_eq!(decoded.version, MANIFEST_FORMAT_VERSION_V1);

	// Overwrite the on-disk file with legacy bytes
	fs::write(&manifest_path, &legacy_bytes).expect("write legacy manifest");

	// Open the tree with legacy manifest
	let tree2 = Tree::new(Arc::clone(&opts)).expect("reopen with legacy manifest");
	let val = get_key(&tree2, b"key_legacy");
	assert_eq!(val, Some(b"val_legacy".to_vec()));

	// Perform another write and flush to rewrite the manifest
	set_key(&tree2, b"key_new", b"val_new").await;
	tree2.flush().expect("flush");
	tree2.close().await.expect("close tree2");

	// Verify that the manifest on disk now has the checksum trailer again
	let upgraded = fs::read(&manifest_path).expect("read upgraded manifest");
	assert_eq!(&upgraded[upgraded.len() - 8..], &MANIFEST_TRAILER_MAGIC);
	let decoded_upgraded = DecodedManifest::decode(upgraded).expect("decode upgraded");
	assert_eq!(decoded_upgraded.version, MANIFEST_FORMAT_VERSION_V1);
}

#[tokio::test]
async fn test_legacy_decoder_compatibility() {
	let temp_dir = tempfile::tempdir().expect("temp dir");
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let tree = Tree::new(Arc::clone(&opts)).expect("open tree");
	set_key(&tree, b"k1", b"v1").await;
	tree.flush().expect("flush");
	tree.close().await.expect("close");

	let manifest_path = opts.manifest_file_path(0);
	let raw_bytes = fs::read(&manifest_path).expect("read manifest");

	// New decoder
	let new_decoded = DecodedManifest::decode(raw_bytes.clone()).expect("new decode");

	// Legacy 0.21.4 decoder on the exact same checksummed bytes
	let legacy_decoded = decode_legacy_manifest_v0_21_4(&raw_bytes).expect("legacy decode");

	assert_eq!(new_decoded.version, legacy_decoded.version);
	assert_eq!(new_decoded.next_table_id, legacy_decoded.next_table_id);
	assert_eq!(new_decoded.log_number, legacy_decoded.log_number);
	assert_eq!(new_decoded.last_sequence, legacy_decoded.last_sequence);
	assert_eq!(new_decoded.level_data, legacy_decoded.level_data);
	assert_eq!(new_decoded.snapshots.len(), legacy_decoded.snapshots.len());
}

#[tokio::test]
async fn test_trailing_garbage_refused() {
	let temp_dir = tempfile::tempdir().expect("temp dir");
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let tree = Tree::new(Arc::clone(&opts)).expect("open tree");
	set_key(&tree, b"k", b"v").await;
	tree.flush().expect("flush");
	tree.close().await.expect("close");

	let manifest_path = opts.manifest_file_path(0);
	let original = fs::read(&manifest_path).expect("read manifest");

	// Append various lengths of trailing garbage
	for extra_len in [1, 5, 15, 17, 32] {
		let mut with_garbage = original.clone();
		with_garbage.extend(vec![0xAA; extra_len]);

		let err = DecodedManifest::decode(with_garbage).unwrap_err();
		match err {
			Error::ManifestCorruption(msg) => {
				assert!(msg.contains("trailing data"), "Expected trailing data error, got {msg}");
			}
			other => panic!("Expected ManifestCorruption, got {other:?}"),
		}
	}
}

#[tokio::test]
async fn test_trailer_corruption_details() {
	let temp_dir = tempfile::tempdir().expect("temp dir");
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let tree = Tree::new(Arc::clone(&opts)).expect("open tree");
	set_key(&tree, b"k", b"v").await;
	tree.flush().expect("flush");
	tree.close().await.expect("close");

	let manifest_path = opts.manifest_file_path(0);
	let original = fs::read(&manifest_path).expect("read manifest");
	let total_len = original.len();

	// 1. Corrupt magic (last 8 bytes)
	let mut bad_magic = original.clone();
	bad_magic[total_len - 1] ^= 0xFF;
	let err = DecodedManifest::decode(bad_magic).unwrap_err();
	match err {
		Error::ManifestCorruption(msg) => {
			assert!(msg.contains("magic mismatch"), "Expected magic mismatch, got {msg}");
		}
		other => panic!("Expected ManifestCorruption, got {other:?}"),
	}

	// 2. Corrupt body length (bytes -16..-12)
	let mut bad_len = original.clone();
	bad_len[total_len - 16] ^= 0xFF;
	let err = DecodedManifest::decode(bad_len).unwrap_err();
	match err {
		Error::ManifestCorruption(msg) => {
			assert!(
				msg.contains("body length mismatch"),
				"Expected body length mismatch, got {msg}"
			);
		}
		other => panic!("Expected ManifestCorruption, got {other:?}"),
	}

	// 3. Corrupt CRC (bytes -12..-8)
	let mut bad_crc = original;
	bad_crc[total_len - 12] ^= 0xFF;
	let err = DecodedManifest::decode(bad_crc).unwrap_err();
	match err {
		Error::ManifestCorruption(msg) => {
			assert!(msg.contains("checksum mismatch"), "Expected checksum mismatch, got {msg}");
		}
		other => panic!("Expected ManifestCorruption, got {other:?}"),
	}
}

#[tokio::test]
async fn test_stale_manifest_temp_files_swept_on_open() {
	let temp_dir = tempfile::tempdir().expect("temp dir");
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let tree = Tree::new(Arc::clone(&opts)).expect("open tree");
	tree.close().await.expect("close tree");

	let manifest_dir = opts.manifest_dir();
	let stale_temp1 = manifest_dir.join(".tmp_12345_67890");
	let stale_temp2 = manifest_dir.join(".tmp_99999_11111");
	let keep_file = manifest_dir.join("other_file");

	fs::write(&stale_temp1, b"partial write 1").expect("write temp1");
	fs::write(&stale_temp2, b"partial write 2").expect("write temp2");
	fs::write(&keep_file, b"should stay").expect("write keep");

	assert!(stale_temp1.exists());
	assert!(stale_temp2.exists());
	assert!(keep_file.exists());

	// Reopening the tree should sweep the stale .tmp_* files
	let tree2 = Tree::new(Arc::clone(&opts)).expect("reopen tree");
	tree2.close().await.expect("close tree2");

	assert!(!stale_temp1.exists(), "stale_temp1 was not swept");
	assert!(!stale_temp2.exists(), "stale_temp2 was not swept");
	assert!(keep_file.exists(), "keep_file was unexpectedly deleted");
}
