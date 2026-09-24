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
