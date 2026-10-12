//! The reader against databases written by the real RocksDB (the C++ 11.0.0 fork SurrealDB links),
//! each with a dump of what RocksDB itself returned: every version with its timestamp for the
//! versioned ones (`V` lines), and the latest scan (`S` lines).
//!
//! Run with `SKV_ROCKSDB_FIXTURES=<dir of scenario dirs> cargo test -- --ignored`; the dirs are
//! written by the fixture generator and each holds `db/`, `dump.tsv` and `scenario.txt`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use surrealkv_compat_rocksdb::{Payload, RocksDb, VersionKind};

fn fnv(bytes: &[u8]) -> String {
	let mut hash: u64 = 0xcbf29ce484222325;
	for byte in bytes {
		hash ^= *byte as u64;
		hash = hash.wrapping_mul(0x100000001b3);
	}
	format!("{hash:016x}")
}

fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `(key, timestamp, is delete, value length, value hash)`
type Version = (String, u64, bool, usize, String);

struct Dump {
	/// Set when RocksDB's own iterator stopped on an error, so that the dump is only a prefix.
	error: Option<String>,
	/// Every version, deduplicated by (key, timestamp) keeping the newest sequence number.
	versions: Vec<Version>,
	/// The latest value of every live key.
	latest: BTreeMap<String, (usize, String)>,
}

fn load_dump(dir: &Path) -> Dump {
	let text = fs::read_to_string(dir.join("dump.tsv")).unwrap();
	let mut versions: Vec<(String, u64, bool, usize, String)> = Vec::new();
	let mut latest = BTreeMap::new();
	let mut error = None;
	for line in text.lines() {
		let f: Vec<&str> = line.split('\t').collect();
		match f[0] {
			"E" => error = Some(f[1].to_string()),
			"V" => {
				// V key ts seq type vlen vfnv rlen rfnv
				let kind: u8 = f[4].parse().unwrap();
				let is_delete = kind == 0 || kind == 0x14;
				versions.push((
					f[1].to_string(),
					f[2].parse().unwrap(),
					is_delete,
					f[5].parse().unwrap(),
					f[6].to_string(),
				));
			}
			"S" => {
				latest.insert(f[1].to_string(), (f[2].parse().unwrap(), f[3].to_string()));
			}
			_ => {}
		}
	}
	// RocksDB lists a key's versions newest timestamp first, then newest sequence first: of the
	// versions with one timestamp only the first is the one it serves.
	let mut out: Vec<Version> = Vec::new();
	for (key, ts, del, len, hash) in versions {
		if let Some(last) = out.last() {
			if last.0 == key && last.1 == ts {
				continue;
			}
		}
		out.push((key, ts, del, len, hash));
	}
	Dump {
		error,
		versions: out,
		latest,
	}
}

fn scenarios(root: &Path) -> Vec<(String, PathBuf, bool)> {
	let mut out = Vec::new();
	for entry in fs::read_dir(root).unwrap().flatten() {
		let dir = entry.path();
		if dir.join("db").is_dir() && dir.join("dump.tsv").is_file() {
			let text = fs::read_to_string(dir.join("scenario.txt")).unwrap_or_default();
			let versioned = text.contains("versioned=true");
			out.push((entry.file_name().to_string_lossy().to_string(), dir, versioned));
		}
	}
	out.sort();
	out
}

fn check(dir: &Path, versioned: bool) -> Result<String, String> {
	let db = RocksDb::open(&dir.join("db")).map_err(|e| format!("open: {e}"))?;
	if db.has_user_timestamps() != versioned {
		return Err(format!("has_user_timestamps = {}", db.has_user_timestamps()));
	}
	let mut blobs = db.blob_files();
	let mut got: Vec<Version> = Vec::new();
	for version in db.versions().map_err(|e| format!("versions: {e}"))? {
		let version = version.map_err(|e| format!("stream: {e}"))?;
		let (len, hash) = match &version.payload {
			Payload::Inline(v) => (v.len(), fnv(v)),
			Payload::Blob(r) => {
				let v = blobs.read(r, &version.user_key).map_err(|e| format!("blob: {e}"))?;
				(v.len(), fnv(&v))
			}
			Payload::None => (0, fnv(&[])),
		};
		got.push((
			hex(&version.user_key),
			version.timestamp.unwrap_or(0),
			version.kind == VersionKind::Delete,
			len,
			hash,
		));
	}
	let dump = load_dump(dir);

	if versioned && dump.error.is_some() {
		// RocksDB's iterator gave up at an entry it could not read; what it returned before that
		// is a prefix of what the reader finds.
		if got.len() < dump.versions.len() || got[..dump.versions.len()] != dump.versions[..] {
			return Err(
				"the versions RocksDB returned are not a prefix of the reader's".to_string()
			);
		}
		return Ok(format!(
			"{:6} versions (RocksDB itself stops after {}: {})",
			got.len(),
			dump.versions.len(),
			dump.error.unwrap_or_default()
		));
	}
	if versioned && got != dump.versions {
		let first = got.iter().zip(dump.versions.iter()).position(|(a, b)| a != b);
		return Err(format!(
			"versions differ: got {}, expected {}; first difference at {:?}: got {:?} expected {:?}",
			got.len(),
			dump.versions.len(),
			first,
			first.map(|i| &got[i]),
			first.map(|i| &dump.versions[i]),
		));
	}

	// The latest value of every key: the newest version of it, if that is not a delete.
	let mut latest: BTreeMap<String, (usize, String)> = BTreeMap::new();
	let mut seen = BTreeSet::new();
	for (key, _ts, is_delete, len, hash) in &got {
		if !seen.insert(key.clone()) {
			continue;
		}
		if !is_delete {
			latest.insert(key.clone(), (*len, hash.clone()));
		}
	}
	if latest != dump.latest {
		let missing: Vec<_> =
			dump.latest.keys().filter(|k| !latest.contains_key(*k)).take(3).collect();
		let extra: Vec<_> =
			latest.keys().filter(|k| !dump.latest.contains_key(*k)).take(3).collect();
		return Err(format!(
			"latest differs: got {} keys, expected {}; missing {missing:?}, extra {extra:?}",
			latest.len(),
			dump.latest.len(),
		));
	}
	Ok(format!(
		"{:6} versions {:5} live keys {} tables {} logs{}",
		got.len(),
		latest.len(),
		db.table_count(),
		db.wal_files.len(),
		if db.wal_damaged {
			", wal damaged"
		} else {
			""
		}
	))
}

#[test]
#[ignore = "needs SKV_ROCKSDB_FIXTURES"]
fn every_generated_database_reads_as_rocksdb_returned() {
	let root = PathBuf::from(std::env::var("SKV_ROCKSDB_FIXTURES").expect("SKV_ROCKSDB_FIXTURES"));
	let mut failures = Vec::new();
	for (name, dir, versioned) in scenarios(&root) {
		match check(&dir, versioned) {
			Ok(summary) => println!("{name:30} ok   {summary}"),
			Err(e) => {
				println!("{name:30} FAILED {e}");
				failures.push(name);
			}
		}
	}
	assert!(failures.is_empty(), "{failures:?}");
}
