use tempfile::TempDir;

use super::*;

#[test]
fn test_dump_export_and_read_roundtrip() {
	let temp_dir = TempDir::new().unwrap();
	let dump_path = temp_dir.path().join("indxdb_dump.bin");

	let original_entries = vec![
		(b"users:0001".to_vec(), b"Alice".to_vec()),
		(b"users:0002".to_vec(), b"Bob".to_vec()),
		(b"config:app".to_vec(), b"{\"version\": 2}".to_vec()),
	];

	export_dump(&dump_path, &original_entries).expect("Export should succeed");
	assert!(is_indxdb_dump_dir(temp_dir.path()));

	let recovered = read_dump(&dump_path).expect("Read dump should succeed");
	assert_eq!(recovered.len(), 3);
	assert_eq!(recovered, original_entries);
}

#[test]
fn test_dump_bad_magic() {
	let temp_dir = TempDir::new().unwrap();
	let dump_path = temp_dir.path().join("bad_dump.bin");

	std::fs::write(&dump_path, b"NOT_INDXDB_DATA").unwrap();
	let res = read_dump(&dump_path);
	assert!(res.is_err());
}
