use std::process::Command;

use tempfile::TempDir;

#[test]
fn test_cli_basic_workflow() {
	let temp_dir = TempDir::new().unwrap();
	let db_path = temp_dir.path().to_str().unwrap();

	// Binary path from cargo test runner
	let bin = env!("CARGO_BIN_EXE_skv");

	// 1. Put key-value pairs
	let out = Command::new(bin)
		.args(["put", db_path, "test:key1", "value1"])
		.output()
		.expect("Failed to execute put");
	assert!(out.status.success());

	let out = Command::new(bin)
		.args(["put", db_path, "test:key2", "value2"])
		.output()
		.expect("Failed to execute put");
	assert!(out.status.success());

	// 2. Get key
	let out = Command::new(bin)
		.args(["get", db_path, "test:key1"])
		.output()
		.expect("Failed to execute get");
	assert!(out.status.success());
	let val = String::from_utf8_lossy(&out.stdout);
	assert_eq!(val.trim(), "value1");

	// 3. Scan
	let out = Command::new(bin)
		.args(["scan", db_path, "--prefix", "test:"])
		.output()
		.expect("Failed to execute scan");
	assert!(out.status.success());
	let scan_res = String::from_utf8_lossy(&out.stdout);
	assert!(scan_res.contains("test:key1 => value1"));
	assert!(scan_res.contains("test:key2 => value2"));

	// 4. Inspect
	let out =
		Command::new(bin).args(["inspect", db_path]).output().expect("Failed to execute inspect");
	assert!(out.status.success());
	let inspect_res = String::from_utf8_lossy(&out.stdout);
	assert!(inspect_res.contains("SurrealKV Database Inspection"));
	assert!(inspect_res.contains("Total SSTables:"));

	// 5. Manifest
	let out =
		Command::new(bin).args(["manifest", db_path]).output().expect("Failed to execute manifest");
	assert!(out.status.success());
	let manifest_res = String::from_utf8_lossy(&out.stdout);
	assert!(manifest_res.contains("LevelManifest State"));

	// 6. Scrub
	let out = Command::new(bin).args(["scrub", db_path]).output().expect("Failed to execute scrub");
	assert!(out.status.success());
	let scrub_res = String::from_utf8_lossy(&out.stdout);
	assert!(scrub_res.contains("SCRUB PASSED"));

	// 7. Delete key
	let out = Command::new(bin)
		.args(["delete", db_path, "test:key1"])
		.output()
		.expect("Failed to execute delete");
	assert!(out.status.success());

	// 8. Verify deletion via get (should exit with code 1)
	let out = Command::new(bin)
		.args(["get", db_path, "test:key1"])
		.output()
		.expect("Failed to execute get");
	assert!(!out.status.success());
}

/// A file that is a sealed table as far as a tool without a key can tell: bytes, and a footer
/// whose checksum type says that the blocks of the table are sealed.
fn sealed_table_without_a_key() -> Vec<u8> {
	let mut footer = vec![0u8; 50];
	footer[0] = 1; // format
	footer[1] = 2; // checksum type: sealed blocks
	footer[2..6].copy_from_slice(&[0, 5, 0, 5]); // the handles of the meta-index and the index
	footer[42..].copy_from_slice(&[0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb]);
	let mut file = vec![0u8; 300];
	file.extend_from_slice(&footer);
	file
}

#[test]
fn test_cli_sst_refuses_a_sealed_table_and_says_why() {
	let temp_dir = TempDir::new().unwrap();
	let path = temp_dir.path().join("sealed.sst");
	std::fs::write(&path, sealed_table_without_a_key()).unwrap();

	let out = Command::new(env!("CARGO_BIN_EXE_skv"))
		.args(["sst", path.to_str().unwrap()])
		.output()
		.expect("Failed to execute sst");

	// An error and not a panic, which would exit with 101.
	assert_eq!(out.status.code(), Some(1));
	let stderr = String::from_utf8_lossy(&out.stderr);
	assert!(stderr.contains("no key manager"), "{stderr}");
	assert!(!stderr.contains("panicked"), "{stderr}");
}

#[test]
fn test_cli_scrub_does_not_pass_a_database_with_a_sealed_table() {
	let temp_dir = TempDir::new().unwrap();
	let tables = temp_dir.path().join("sstables");
	std::fs::create_dir_all(&tables).unwrap();
	std::fs::write(tables.join(format!("{:020}.sst", 1)), sealed_table_without_a_key()).unwrap();

	let out = Command::new(env!("CARGO_BIN_EXE_skv"))
		.args(["scrub", temp_dir.path().to_str().unwrap()])
		.output()
		.expect("Failed to execute scrub");

	// It did not verify the table, and it does not say that the table is damaged.
	assert_eq!(out.status.code(), Some(1));
	assert!(!String::from_utf8_lossy(&out.stdout).contains("SCRUB PASSED"));
	let stderr = String::from_utf8_lossy(&out.stderr);
	assert!(stderr.contains("SCRUB INCOMPLETE"), "{stderr}");
	assert!(!stderr.contains("SCRUB FAILED"), "{stderr}");
}
