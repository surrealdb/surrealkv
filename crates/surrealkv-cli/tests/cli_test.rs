use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use surrealkv::tde::{seal_manifest, DbId, EncryptionContext, FileKind};
use surrealkv::{CipherSuite, SoftwareKeyManager};
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

	// 6b. The encryption of a database in the clear: none, and nothing is inconsistent
	let out =
		Command::new(bin).args(["enc-info", db_path]).output().expect("Failed to execute enc-info");
	assert_eq!(out.status.code(), Some(0), "{}", String::from_utf8_lossy(&out.stderr));
	let info = String::from_utf8_lossy(&out.stdout);
	let line = info.lines().find(|line| line.starts_with("Encrypted:")).expect("an Encrypted line");
	assert!(line.contains("no"), "{line}");

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

// ========== Encrypted databases ==========
//
// The fixtures below are directories of real files as far as the first bytes of each go: a manifest
// that is sealed, and tables, value-log files and log segments that begin with a real encryption
// header for the database. What follows the header is not data, so they can be described and
// refused and not read, which is all that these tests ask of them: reading a database with its key
// is for the build that has the preview feature, below.

/// A root key whose bytes are none of them text, so that no form of it can be mistaken for
/// anything else that the tool prints.
fn root_key() -> [u8; 32] {
	std::array::from_fn(|i| 0xC0 + i as u8)
}

fn root_key_hex() -> String {
	root_key().iter().map(|byte| format!("{byte:02x}")).collect()
}

fn context(db: u8, suite: CipherSuite) -> EncryptionContext {
	EncryptionContext::new(
		Arc::new(SoftwareKeyManager::new(root_key())),
		DbId::from_bytes([db; 16]),
		suite,
	)
}

/// The 50 bytes at the end of a table whose footer says that its blocks are sealed (checksum type
/// 2), or in the clear (checksum type 1).
fn footer(checksum_type: u8) -> Vec<u8> {
	let mut footer = vec![0u8; 50];
	footer[0] = 1; // format
	footer[1] = checksum_type;
	footer[2..6].copy_from_slice(&[0, 5, 0, 5]); // the handles of the meta-index and the index
	footer[42..].copy_from_slice(&[0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb]);
	footer
}

fn sealed_table(ctx: &EncryptionContext, id: u64) -> Vec<u8> {
	let (header, _) = ctx.create_file(FileKind::Sst, id, &[]).unwrap();
	let mut file = header.encode().to_vec();
	file.extend_from_slice(&[0u8; 200]);
	file.extend_from_slice(&footer(2));
	file
}

fn plain_table() -> Vec<u8> {
	let mut file = vec![0xABu8; 300];
	file.extend_from_slice(&footer(1));
	file
}

/// A file of the value log: the 31 byte file header with the version of a sealed file, its
/// checksum, and the encryption header, which is what a sealed one begins with.
fn sealed_vlog(ctx: &EncryptionContext, id: u32) -> Vec<u8> {
	let mut fixed = [0u8; 31];
	fixed[..4].copy_from_slice(b"VLOG");
	fixed[4..6].copy_from_slice(&[0x80, 0x01]);
	fixed[6..10].copy_from_slice(&id.to_be_bytes());
	let (header, _) = ctx.create_file(FileKind::Vlog, u64::from(id), &fixed).unwrap();
	let mut file = fixed.to_vec();
	file.extend_from_slice(&crc32fast::hash(&fixed).to_be_bytes());
	file.extend_from_slice(&header.encode());
	file
}

/// A segment of the log that holds only its header record: the framing of a record, whose type is
/// 10, and the encryption header and the two bytes that its tag covers.
fn sealed_wal(ctx: &EncryptionContext, id: u64) -> Vec<u8> {
	let (header, _) = ctx.create_file(FileKind::Wal, id, &[1, 0]).unwrap();
	let mut file = vec![0u8; 4];
	file.extend_from_slice(&94u16.to_be_bytes());
	file.push(10);
	file.extend_from_slice(&header.encode());
	file.extend_from_slice(&[1, 0]);
	file
}

/// A database directory of the files above, all of one database and sealed under key 1.
fn encrypted_database(db: u8, suite: CipherSuite) -> TempDir {
	let dir = TempDir::new().unwrap();
	let root = dir.path();
	for sub in ["manifest", "sstables", "vlog", "wal"] {
		fs::create_dir_all(root.join(sub)).unwrap();
	}
	let ctx = context(db, suite);
	fs::write(
		root.join("manifest").join(format!("{:020}.manifest", 0)),
		seal_manifest(&ctx, b"the body of a manifest").unwrap(),
	)
	.unwrap();
	fs::write(root.join("sstables").join(format!("{:020}.sst", 1)), sealed_table(&ctx, 1)).unwrap();
	fs::write(root.join("vlog").join(format!("{:020}.vlog", 1)), sealed_vlog(&ctx, 1)).unwrap();
	fs::write(root.join("wal").join(format!("{:020}.wal", 1)), sealed_wal(&ctx, 1)).unwrap();
	dir
}

/// Every file under `root` with its bytes, and every directory.
fn snapshot(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
	fn visit(dir: &Path, found: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
		for entry in fs::read_dir(dir).unwrap().flatten() {
			let path = entry.path();
			if entry.file_type().unwrap().is_dir() {
				found.insert(path.clone(), None);
				visit(&path, found);
			} else {
				found.insert(path.clone(), Some(fs::read(&path).unwrap()));
			}
		}
	}
	let mut found = BTreeMap::new();
	visit(root, &mut found);
	found
}

/// A key file with exactly `mode`, whatever the umask of the process is.
fn key_file(dir: &Path, name: &str, bytes: &[u8], mode: u32) -> PathBuf {
	let path = dir.join(name);
	fs::write(&path, bytes).unwrap();
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;

		fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
	}
	#[cfg(not(unix))]
	let _ = mode;
	path
}

/// The tool, with no key file named by the environment of whoever runs the tests: a test that
/// wants one sets it itself, and one that does not must not find the one of a developer.
fn skv_command() -> Command {
	let mut command = Command::new(env!("CARGO_BIN_EXE_skv"));
	command.env_remove("SKV_KEY_FILE");
	command
}

fn skv(args: &[&str]) -> Output {
	skv_command().args(args).output().expect("Failed to execute skv")
}

fn stdout(out: &Output) -> String {
	String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
	String::from_utf8_lossy(&out.stderr).into_owned()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
	haystack.windows(needle.len()).any(|window| window == needle)
}

/// Fails if the key shows up in what a run printed, in any form that a formatter or a log line
/// could give it: the bytes, the hex digits in either case, or the numbers.
fn assert_no_key(out: &Output, what: &str) {
	let key = root_key();
	let hex = root_key_hex();
	let decimal = key.iter().map(|byte| byte.to_string()).collect::<Vec<_>>();
	for (stream, bytes) in [("stdout", &out.stdout), ("stderr", &out.stderr)] {
		assert!(!contains(bytes, &key), "{what}: the key is in {stream}");
		assert!(!contains(bytes, hex.as_bytes()), "{what}: the key in hex is in {stream}");
		assert!(
			!contains(bytes, hex.to_uppercase().as_bytes()),
			"{what}: the key in capitals is in {stream}"
		);
		for separator in [", ", ","] {
			let listed = decimal.join(separator);
			assert!(
				!contains(bytes, listed.as_bytes()),
				"{what}: the key as numbers is in {stream}"
			);
		}
		// Not even the start of it, in either form.
		assert!(
			!contains(bytes, &hex.as_bytes()[..16]),
			"{what}: a part of the key is in {stream}"
		);
	}
}

#[test]
fn test_cli_enc_info_describes_an_encrypted_database_without_a_key() {
	let db = encrypted_database(0xD1, CipherSuite::XChaCha20Poly1305);
	let size_of =
		|sub: &str, name: String| fs::metadata(db.path().join(sub).join(name)).unwrap().len();
	let before = snapshot(db.path());

	let out = skv(&["enc-info", db.path().to_str().unwrap()]);
	let text = stdout(&out);
	assert_eq!(out.status.code(), Some(0), "{text}{}", stderr(&out));
	let line = text.lines().find(|line| line.starts_with("Encrypted:")).expect("an Encrypted line");
	assert!(line.contains("yes"), "{line}");
	assert!(text.contains("Cipher suite:     XChaCha20-Poly1305"), "{text}");
	assert!(text.contains(&format!("Database id:      {}", "d1".repeat(16))), "{text}");
	assert!(text.contains("Manifest key id:  1"), "{text}");
	assert!(text.contains("Key 1:"), "{text}");
	let manifest = size_of("manifest", format!("{:020}.manifest", 0));
	let table = size_of("sstables", format!("{:020}.sst", 1));
	let log = size_of("vlog", format!("{:020}.vlog", 1));
	let wal = size_of("wal", format!("{:020}.wal", 1));
	assert!(text.contains(&format!("manifest: 1 file(s), {manifest} bytes")), "{text}");
	assert!(text.contains(&format!("tables: 1 file(s), {table} bytes")), "{text}");
	assert!(text.contains(&format!("value log: 1 file(s), {log} bytes")), "{text}");
	assert!(text.contains(&format!("WAL: 1 file(s), {wal} bytes")), "{text}");
	assert!(!text.contains("WARNING"), "{text}");

	assert_eq!(snapshot(db.path()), before, "enc-info writes nothing");
	assert_no_key(&out, "enc-info");
}

#[test]
fn test_cli_enc_info_is_non_zero_for_a_directory_that_is_not_consistent() {
	let db = encrypted_database(0xD1, CipherSuite::XChaCha20Poly1305);
	let tables = db.path().join("sstables");
	// A table in the clear, and a table of another database.
	fs::write(tables.join(format!("{:020}.sst", 2)), plain_table()).unwrap();
	fs::write(
		tables.join(format!("{:020}.sst", 3)),
		sealed_table(&context(0xD2, CipherSuite::XChaCha20Poly1305), 3),
	)
	.unwrap();
	let before = snapshot(db.path());

	let out = skv(&["enc-info", db.path().to_str().unwrap()]);
	let text = stdout(&out);
	// Not 0, which is a consistent directory, not 1, which is a directory that cannot be read, and
	// not 2, which is the status of a command line that is not one.
	assert_eq!(out.status.code(), Some(3), "{text}{}", stderr(&out));
	assert!(text.contains("WARNING"), "{text}");
	assert!(
		text.contains("sstables/00000000000000000002.sst is in the clear in an encrypted database"),
		"{text}"
	);
	assert!(
		text.contains(&format!(
			"sstables/00000000000000000003.sst belongs to another database, {}",
			"d2".repeat(16)
		)),
		"{text}"
	);
	assert!(stderr(&out).contains("INCONSISTENT"), "{}", stderr(&out));
	assert_eq!(snapshot(db.path()), before);
}

#[test]
fn test_cli_enc_info_says_what_it_cannot_read() {
	let dir = TempDir::new().unwrap();
	let out = skv(&["enc-info", dir.path().to_str().unwrap()]);
	assert_eq!(out.status.code(), Some(1));
	assert!(stderr(&out).contains("no manifest"), "{}", stderr(&out));
	assert!(snapshot(dir.path()).is_empty());
}

#[test]
fn test_cli_refuses_an_encrypted_database_without_a_key_and_changes_nothing() {
	let db = encrypted_database(0xD1, CipherSuite::Aes256Gcm);
	let path = db.path().to_str().unwrap().to_string();
	let table = db.path().join("sstables").join(format!("{:020}.sst", 1));
	let table = table.to_str().unwrap().to_string();
	let before = snapshot(db.path());

	let commands: Vec<Vec<&str>> = vec![
		vec!["inspect", &path],
		vec!["manifest", &path],
		vec!["wal", &path],
		vec!["sst", &table],
		vec!["scrub", &path],
		vec!["scan", &path],
		vec!["get", &path, "k"],
		vec!["put", &path, "k", "v"],
		vec!["delete", &path, "k"],
		vec!["bench", &path, "--keys", "8", "--concurrency", "2"],
	];
	for args in commands {
		let out = skv(&args);
		let what = args[0];
		// A refusal and not a crash, which would exit with 101.
		assert_eq!(out.status.code(), Some(1), "{what}: {}{}", stdout(&out), stderr(&out));
		let err = stderr(&out);
		assert!(!err.contains("panicked"), "{what}: {err}");
		assert!(err.contains("encrypted"), "{what}: {err}");
		assert!(err.contains("--key-file"), "{what}: {err}");
		assert!(!stdout(&out).contains("PASSED"), "{what}: {}", stdout(&out));
		assert_eq!(snapshot(db.path()), before, "{what} changed the database");
	}
}

#[test]
fn test_cli_refuses_to_migrate_into_an_encrypted_database_and_changes_nothing() {
	let destination = encrypted_database(0xD1, CipherSuite::Aes256Gcm);
	let source = TempDir::new().unwrap();
	let before = snapshot(destination.path());

	// The destination is looked at before the source is, so that nothing is read for a migration
	// that cannot be done, and the source here is not even a store.
	let out =
		skv(&["migrate", source.path().to_str().unwrap(), destination.path().to_str().unwrap()]);
	assert_eq!(out.status.code(), Some(1), "{}{}", stdout(&out), stderr(&out));
	let err = stderr(&out);
	assert!(err.contains("is an encrypted database"), "{err}");
	assert!(err.contains("never done into an encrypted database"), "{err}");
	assert!(!err.contains("not a recognized"), "{err}");
	assert_eq!(snapshot(destination.path()), before);
	assert!(snapshot(source.path()).is_empty());
}

#[test]
fn test_cli_a_key_file_that_is_not_a_key_is_refused_with_the_reason() {
	let db = encrypted_database(0xD1, CipherSuite::Aes256Gcm);
	let table = db.path().join("sstables").join(format!("{:020}.sst", 1));
	let table = table.to_str().unwrap();
	let keys = TempDir::new().unwrap();
	let before = snapshot(db.path());

	let short = key_file(keys.path(), "short.key", &root_key()[..31], 0o600);
	let out = skv(&["sst", table, "--key-file", short.to_str().unwrap()]);
	assert_eq!(out.status.code(), Some(1));
	let err = stderr(&out);
	assert!(err.contains("Failed to load the key file"), "{err}");
	assert!(err.contains("a root key is"), "{err}");
	assert_no_key(&out, "a key that is too short");

	// A file that is not there says so.
	let missing = keys.path().join("none.key");
	let out = skv(&["sst", table, "--key-file", missing.to_str().unwrap()]);
	assert_eq!(out.status.code(), Some(1));
	assert!(stderr(&out).contains("Failed to load the key file"), "{}", stderr(&out));

	// And so does the environment variable, which names a file and is read when there is no flag.
	let out = skv_command().args(["sst", table]).env("SKV_KEY_FILE", &short).output().unwrap();
	assert_eq!(out.status.code(), Some(1));
	assert!(stderr(&out).contains("Failed to load the key file"), "{}", stderr(&out));
	assert_eq!(snapshot(db.path()), before);
}

#[cfg(unix)]
#[test]
fn test_cli_a_key_file_that_every_user_can_read_is_refused_unless_that_is_chosen() {
	let db = encrypted_database(0xD1, CipherSuite::Aes256Gcm);
	let table = db.path().join("sstables").join(format!("{:020}.sst", 1));
	let table = table.to_str().unwrap();
	let keys = TempDir::new().unwrap();
	let open = key_file(keys.path(), "open.key", format!("{}\n", root_key_hex()).as_bytes(), 0o644);
	let open_path = open.to_str().unwrap();

	let out = skv(&["sst", table, "--key-file", open_path]);
	assert_eq!(out.status.code(), Some(1));
	let err = stderr(&out);
	assert!(err.contains("can be reached by every user"), "{err}");
	assert!(err.contains("chmod 600"), "{err}");
	assert_no_key(&out, "an open key file");

	// Said so, the key is read, and what comes next is for the rest of the tool: in a build
	// without the preview feature, the guard that no build of this tool gets round.
	let out = skv(&["sst", table, "--key-file", open_path, "--allow-insecure-key-file"]);
	assert_eq!(out.status.code(), Some(1));
	assert!(!stderr(&out).contains("can be reached by every user"), "{}", stderr(&out));
	assert_no_key(&out, "an open key file that is accepted");
}

#[cfg(not(feature = "encryption-preview"))]
#[test]
fn test_cli_without_the_preview_feature_a_key_is_refused_by_the_guard_and_the_flag_wins() {
	let db = encrypted_database(0xD1, CipherSuite::Aes256Gcm);
	let table = db.path().join("sstables").join(format!("{:020}.sst", 1));
	let table = table.to_str().unwrap();
	let keys = TempDir::new().unwrap();
	let good = key_file(keys.path(), "good.key", &root_key(), 0o600);
	let hex = key_file(keys.path(), "hex.key", format!("{}\n", root_key_hex()).as_bytes(), 0o600);
	let bad = key_file(keys.path(), "bad.key", &root_key()[..31], 0o600);
	let before = snapshot(db.path());

	for (what, key) in [("raw", &good), ("hex", &hex)] {
		let out = skv_command()
			.args(["sst", table, "--key-file", key.to_str().unwrap()])
			.env("RUST_LOG", "trace")
			.output()
			.unwrap();
		assert_eq!(out.status.code(), Some(1), "{what}");
		let err = stderr(&out);
		assert!(err.contains("not enabled in this build"), "{what}: {err}");
		assert!(err.contains("encryption-preview"), "{what}: {err}");
		assert_no_key(&out, what);
	}

	// The flag is read before the environment: with a good file in the flag, a bad one in the
	// environment is not looked at, and the answer is the guard's and not a key that is not one.
	let out = skv_command()
		.args(["sst", table, "--key-file", good.to_str().unwrap()])
		.env("SKV_KEY_FILE", &bad)
		.output()
		.unwrap();
	assert_eq!(out.status.code(), Some(1));
	assert!(stderr(&out).contains("not enabled in this build"), "{}", stderr(&out));
	assert!(!stderr(&out).contains("Failed to load the key file"), "{}", stderr(&out));
	assert_eq!(snapshot(db.path()), before);
}

// ========== With the preview feature ==========
//
// A build of the tool with the guard compiled out reads and writes encrypted databases. These run
// in the job that builds it, and make the database with the tool itself.

#[cfg(feature = "encryption-preview")]
mod with_the_preview_feature {
	use super::*;

	/// The suite that the database below is created with is the one that the library picks for the
	/// machine, so what is checked of it is that it is one of the two that exist.
	#[test]
	fn test_cli_creates_reads_scrubs_and_describes_an_encrypted_database_with_its_key() {
		let dir = TempDir::new().unwrap();
		let db = dir.path().join("db");
		let db_path = db.to_str().unwrap();
		let key =
			key_file(dir.path(), "root.key", format!("{}\n", root_key_hex()).as_bytes(), 0o600);
		let key_path = key.to_str().unwrap();

		// Created with the key, by a write.
		for (name, value) in [("k1", "v1"), ("k2", "v2"), ("k3", "v3")] {
			let out = skv(&["put", db_path, name, value, "--key-file", key_path]);
			assert_eq!(out.status.code(), Some(0), "{}{}", stdout(&out), stderr(&out));
			assert_no_key(&out, "put");
		}

		// Read with it, and through the variable that names the file.
		let out = skv(&["get", db_path, "k1", "--key-file", key_path]);
		assert_eq!(stdout(&out).trim(), "v1", "{}", stderr(&out));
		let out = skv_command()
			.args(["scan", db_path, "--prefix", "k"])
			.env("SKV_KEY_FILE", &key)
			.output()
			.unwrap();
		assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
		assert!(stdout(&out).contains("k2 => v2"), "{}", stdout(&out));

		// Without it, refused, and nothing changes.
		let before = snapshot(&db);
		let out = skv(&["get", db_path, "k1"]);
		assert_eq!(out.status.code(), Some(1));
		assert!(stderr(&out).contains("--key-file"), "{}", stderr(&out));
		assert_eq!(snapshot(&db), before);

		// Described without it.
		let out = skv(&["enc-info", db_path]);
		let text = stdout(&out);
		assert_eq!(out.status.code(), Some(0), "{text}{}", stderr(&out));
		assert!(text.contains("Cipher suite:"), "{text}");
		assert!(text.contains("Manifest key id:  1"), "{text}");
		assert!(text.contains("manifest: 1 file(s)"), "{text}");
		assert_no_key(&out, "enc-info");

		// Scrubbed with it: every block of every kind is authenticated.
		let out = skv(&["scrub", db_path, "--key-file", key_path]);
		assert_eq!(out.status.code(), Some(0), "{}{}", stdout(&out), stderr(&out));
		assert!(stdout(&out).contains("SCRUB PASSED"), "{}", stdout(&out));
		assert_no_key(&out, "scrub");

		// With every level of logging on, nothing that a command prints holds the key, in any form,
		// whatever it does with it: reading, listing, verifying, and writing a commit that is
		// sealed and flushed when the process ends.
		let table = fs::read_dir(db.join("sstables")).unwrap().flatten().map(|e| e.path()).min();
		let table = table.expect("a table");
		let table = table.to_str().unwrap();
		let traced: Vec<Vec<&str>> = vec![
			vec!["get", db_path, "k1", "--key-file", key_path],
			vec!["scan", db_path, "--key-file", key_path],
			vec!["inspect", db_path, "--key-file", key_path],
			vec!["manifest", db_path, "--key-file", key_path],
			vec!["wal", db_path, "--key-file", key_path],
			vec!["sst", table, "--dump-keys", "--verify-checksum", "--key-file", key_path],
			vec!["scrub", db_path, "--key-file", key_path],
			vec!["put", db_path, "k4", "v4", "--key-file", key_path],
		];
		for args in traced {
			let out = skv_command().args(&args).env("RUST_LOG", "trace").output().unwrap();
			assert_eq!(out.status.code(), Some(0), "{}: {}{}", args[0], stdout(&out), stderr(&out));
			assert_no_key(&out, args[0]);
		}

		// A byte of the first block of a table is changed: the header of 92 bytes comes first, and
		// the first block follows it. Each write above was a process of its own and flushed on
		// close, so there are several tables, and any one will do.
		let mut tables: Vec<PathBuf> = fs::read_dir(db.join("sstables"))
			.unwrap()
			.flatten()
			.map(|entry| entry.path())
			.collect();
		tables.sort();
		assert!(!tables.is_empty());
		let mut bytes = fs::read(&tables[0]).unwrap();
		bytes[100] ^= 0x01;
		fs::write(&tables[0], &bytes).unwrap();

		// With the key that is damage, and without it nothing can be said.
		let out = skv(&["scrub", db_path, "--key-file", key_path]);
		assert_eq!(out.status.code(), Some(1));
		assert!(stderr(&out).contains("SCRUB FAILED"), "{}", stderr(&out));
		let out = skv(&["scrub", db_path]);
		assert_eq!(out.status.code(), Some(1));
		assert!(stderr(&out).contains("SCRUB INCOMPLETE"), "{}", stderr(&out));
		assert!(!stderr(&out).contains("SCRUB FAILED"), "{}", stderr(&out));

		// Another key does not open it, and says so.
		// Bytes that are not text: a key of 32 hex digits is refused as a mistake.
		let other = key_file(dir.path(), "other.key", &[0x99u8; 32], 0o600);
		let out = skv(&["inspect", db_path, "--key-file", other.to_str().unwrap()]);
		assert_eq!(out.status.code(), Some(1));
		assert!(stderr(&out).contains("does not open"), "{}", stderr(&out));
	}
}

#[test]
fn test_cli_hex_that_is_not_hex_is_an_error_and_not_a_panic() {
	let dir = TempDir::new().unwrap();
	let db = dir.path().join("db");
	// A character of two bytes, a sign that a number may have and a hex digit may not, and an odd
	// length: none of them reaches the database, which is not even created.
	for input in ["\u{e9}\u{e9}", "+f", "abc", "0xzz"] {
		let out = skv(&["get", db.to_str().unwrap(), input, "--hex"]);
		assert_eq!(out.status.code(), Some(1), "{input}: {}", stderr(&out));
		assert!(!stderr(&out).contains("panicked"), "{input}: {}", stderr(&out));
		assert!(stderr(&out).to_lowercase().contains("hex"), "{input}: {}", stderr(&out));
	}
	assert!(!db.exists());
}
