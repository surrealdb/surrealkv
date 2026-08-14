use tempdir::TempDir;

use super::publish::*;

#[test]
fn publish_is_conditional_create_with_idempotent_same_bytes() {
	let temp = TempDir::new("authority").unwrap();
	let dir = temp.path().join("catalog");

	assert_eq!(publish_version(&dir, 1, "catalog", b"alpha").unwrap(), PublishOutcome::Created);
	// Idempotent retry: same bytes are success, not conflict.
	assert_eq!(
		publish_version(&dir, 1, "catalog", b"alpha").unwrap(),
		PublishOutcome::AlreadyExistsSame
	);
	// Divergent bytes for the same version are damage and fail closed.
	let error = publish_version(&dir, 1, "catalog", b"beta")
		.expect_err("divergent republish must fail")
		.to_string();
	assert!(error.contains("different bytes"), "{error}");
	// The loser must not have clobbered the original (rename would have).
	assert_eq!(read_version(&dir, 1, "catalog").unwrap().unwrap(), b"alpha");
	// No temp litter.
	let litter: Vec<_> = std::fs::read_dir(&dir)
		.unwrap()
		.filter_map(|entry| entry.ok())
		.filter(|entry| entry.file_name().to_string_lossy().starts_with('.'))
		.collect();
	assert!(litter.is_empty(), "temp files must be cleaned up: {litter:?}");
}

#[test]
fn latest_resolution_ignores_temp_and_foreign_files() {
	let temp = TempDir::new("authority").unwrap();
	let dir = temp.path().join("root");
	assert_eq!(latest_version_in(&dir, "root").unwrap(), None);

	publish_version(&dir, 1, "root", b"one").unwrap();
	publish_version(&dir, 2, "root", b"two").unwrap();
	std::fs::write(dir.join(".tmp-999-0-junk.root"), b"junk").unwrap();
	std::fs::write(dir.join("not-a-version.root"), b"junk").unwrap();
	std::fs::write(dir.join("00000000000000000003.other"), b"junk").unwrap();

	assert_eq!(latest_version_in(&dir, "root").unwrap(), Some(2));
}

#[test]
fn resolve_from_hint_probes_forward_past_stale_hints() {
	let temp = TempDir::new("authority").unwrap();
	let dir = temp.path().join("branch").join("aa");
	for version in 1..=5u64 {
		publish_version(&dir, version, "state", format!("v{version}").as_bytes()).unwrap();
	}
	// Stale hint (crash after state publish, before root publish).
	let (version, bytes) = resolve_from_hint(&dir, 3, "state", 8).unwrap().unwrap();
	assert_eq!((version, bytes.as_slice()), (5, b"v5".as_slice()));
	// Exact hint.
	let (version, _) = resolve_from_hint(&dir, 5, "state", 8).unwrap().unwrap();
	assert_eq!(version, 5);
	// Nothing at all.
	let empty = temp.path().join("branch").join("bb");
	assert!(resolve_from_hint(&empty, 1, "state", 8).unwrap().is_none());
}
