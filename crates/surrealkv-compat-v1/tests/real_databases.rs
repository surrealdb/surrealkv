//! Reads databases written by the real engines (v0.20.1 and v0.21.4) with this crate alone, and
//! checks what it reads against what those same engines returned when they reopened a copy.
//!
//! `tests/fixtures/legacy/<version>/<scenario>/` at the root of the repository holds a database
//! (`db`) and its `oracle.tsv`. The format of the oracle (keys are hex):
//!
//! * `G <key> <len> <fnv>`: the latest value of a key, or `-` if it has none.
//! * `S <key> <len> <fnv>`: the latest scan, in order.
//! * `H <key> <ts> <seq> <kind> <len> <fnv>`: one version, as `history` reported it.
//! * `A <key> <ts> <len> <fnv>`: what `get_at` returned (not checked here).
//!
//! What is read is what the old engine replayed when it opened the database: the tables the
//! manifest lists, and the log segments from the manifest's log number on, each up to its first
//! damaged record. The newest sequence number of a key wins, whichever table or the log holds it.
//!
//! Larger databases, which are not in the repository, are read by the ignored test over
//! `$SKV_LEGACY_FIXTURES/<scenario>/db`.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use surrealkv_compat_v1::{
	decode_value,
	is_lsmv1_dir,
	read_manifest,
	read_wal_segment,
	table_layout,
	wal_segments,
	Kind,
	Layout,
	TableLayout,
	TableReader,
	Value,
};

type Digest = (usize, String);

fn fnv(bytes: &[u8]) -> String {
	let mut hash: u64 = 0xcbf29ce484222325;
	for byte in bytes {
		hash ^= *byte as u64;
		hash = hash.wrapping_mul(0x100000001b3);
	}
	format!("{hash:016x}")
}

fn unhex(s: &str) -> Vec<u8> {
	(0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

fn digest_field(len: &str, fnv: &str) -> Option<Digest> {
	(len != "-").then(|| (len.parse().unwrap(), fnv.to_string()))
}

struct Row {
	key: Vec<u8>,
	timestamp: u64,
	seq: u64,
	kind: String,
	value: Option<Digest>,
}

#[derive(Default)]
struct Oracle {
	gets: Vec<(Vec<u8>, Option<Digest>)>,
	scan: Vec<(Vec<u8>, Option<Digest>)>,
	history: Vec<Row>,
}

fn load_oracle(path: &Path) -> Oracle {
	let mut oracle = Oracle::default();
	for line in fs::read_to_string(path).unwrap().lines() {
		let f: Vec<&str> = line.split('\t').collect();
		match f[0] {
			"G" => oracle.gets.push((unhex(f[1]), digest_field(f[2], f[3]))),
			"S" => oracle.scan.push((unhex(f[1]), digest_field(f[2], f[3]))),
			"H" => oracle.history.push(Row {
				key: unhex(f[1]),
				timestamp: f[2].parse().unwrap(),
				seq: f[3].parse().unwrap(),
				kind: f[4].to_string(),
				value: digest_field(f[5], f[6]),
			}),
			_ => {}
		}
	}
	oracle
}

/// One version of a key as read from a table or the log.
#[derive(Clone, Debug)]
struct Version {
	kind: Kind,
	timestamp: u64,
	value: Option<Value>,
	from_log: bool,
}

/// What reading a database found.
#[derive(Default, Debug)]
struct Found {
	versions: BTreeMap<Vec<u8>, BTreeMap<u64, Version>>,
	tables: usize,
	entries_in_tables: usize,
	log_segments: usize,
	log_batches: usize,
	log_entries: usize,
	/// Segments that were damaged, and what was wrong.
	damaged: Vec<(u64, String)>,
	/// Segments below the log number, which the old engine did not replay.
	flushed_segments: usize,
	/// Values that are in the value log and were checked by their length only.
	pointers_by_length: usize,
	pointers: usize,
	/// Versions that were read twice: the same key and sequence number in two places.
	copies: usize,
	copies_in_table_and_log: usize,
}

impl Found {
	fn summary(&self) -> String {
		format!(
			"{} tables ({} entries), {} log segments ({} batches, {} entries, {} flushed segments skipped), damage {:?}, {} versions read twice ({} in a table and the log)",
			self.tables,
			self.entries_in_tables,
			self.log_segments,
			self.log_batches,
			self.log_entries,
			self.flushed_segments,
			self.damaged,
			self.copies,
			self.copies_in_table_and_log
		)
	}
}

/// The length of the value a value pointer points at.
fn pointer_value_size(payload: &[u8]) -> usize {
	assert_eq!(payload.len(), 25, "a value pointer");
	u32::from_be_bytes(payload[17..21].try_into().unwrap()) as usize
}

/// Whether two copies of a version hold the same value. A value in the value log is the same value
/// at another offset (the engine wrote it again when it flushed a log it had already flushed),
/// so a pointer is compared by the size and the checksum of what it points at.
fn same_value(a: &Option<Value>, b: &Option<Value>) -> bool {
	match (a, b) {
		(None, None) => true,
		(Some(a), Some(b)) if a.pointer && b.pointer => a.payload[17..] == b.payload[17..],
		(Some(a), Some(b)) if a.pointer || b.pointer => {
			let (pointer, inline) = if a.pointer {
				(a, b)
			} else {
				(b, a)
			};
			pointer_value_size(&pointer.payload) == inline.payload.len()
		}
		(Some(a), Some(b)) => a.payload == b.payload,
		_ => false,
	}
}

fn add_version(found: &mut Found, key: &[u8], seq: u64, version: Version) {
	let slot = found.versions.entry(key.to_vec()).or_default();
	if let Some(existing) = slot.get(&seq) {
		// A copy of a version that two tables hold, or a table and the log, as a compaction or a
		// replay of a log that was already flushed leaves, is one version.
		assert_eq!(existing.kind, version.kind, "{key:02x?} at {seq}");
		assert_eq!(existing.timestamp, version.timestamp, "{key:02x?} at {seq}");
		assert!(same_value(&existing.value, &version.value), "{key:02x?} at {seq}");
		found.copies += 1;
		if existing.from_log != version.from_log {
			found.copies_in_table_and_log += 1;
		}
	} else {
		slot.insert(seq, version);
	}
}

fn read_database(db: &Path) -> Found {
	let layout = Layout::new(db);
	assert!(is_lsmv1_dir(db), "{} is an LSMV1 database", db.display());
	let manifest = read_manifest(&fs::read(layout.manifest_path()).unwrap()).unwrap();
	assert_eq!(manifest.version, 1);
	let mut found = Found::default();

	let mut max_seq = 0;
	for id in manifest.level_data.iter().flatten() {
		let path = layout.sstable_path(*id);
		assert_eq!(table_layout(&path).unwrap(), TableLayout::Lsmv1, "table {id}");
		let table = TableReader::open(&path).unwrap();
		found.tables += 1;
		let mut previous: Option<(Vec<u8>, u64)> = None;
		for entry in table.entries().unwrap() {
			let entry = entry.unwrap_or_else(|e| panic!("table {id}: {e}"));
			// The order the table is stored in: user key ascending, then newest first.
			if let Some((key, seq)) = &previous {
				assert!(
					(key.as_slice(), Reverse(*seq))
						< (entry.user_key.as_slice(), Reverse(entry.seq)),
					"table {id} is not in order at {:02x?} {}",
					entry.user_key,
					entry.seq
				);
			}
			previous = Some((entry.user_key.clone(), entry.seq));
			assert!(
				!matches!(entry.kind, Kind::RangeDelete | Kind::LogData | Kind::Merge),
				"table {id} holds a {:?}",
				entry.kind
			);
			max_seq = max_seq.max(entry.seq);
			found.entries_in_tables += 1;
			let value = decode_value(entry.kind, &entry.value).unwrap_or_else(|e| {
				panic!("table {id}, {:02x?} {}: {e}", entry.user_key, entry.seq)
			});
			add_version(
				&mut found,
				&entry.user_key,
				entry.seq,
				Version {
					kind: entry.kind,
					timestamp: entry.timestamp,
					value,
					from_log: false,
				},
			);
		}
	}
	// What the manifest says is the newest write that a table holds.
	assert_eq!(manifest.last_sequence, max_seq, "the last sequence number of the manifest");

	for segment in wal_segments(&layout.wal_dir()).unwrap() {
		if segment.id < manifest.log_number {
			found.flushed_segments += 1;
			continue;
		}
		found.log_segments += 1;
		let read = read_wal_segment(&segment.path).unwrap();
		if let Some(damage) = read.damage {
			found.damaged.push((segment.id, damage));
		}
		for batch in read.batches {
			found.log_batches += 1;
			for (i, entry) in batch.entries.iter().enumerate() {
				found.log_entries += 1;
				let seq = batch.starting_seq + i as u64;
				assert!(
					!matches!(entry.kind, Kind::RangeDelete | Kind::LogData | Kind::Merge),
					"the log holds a {:?}",
					entry.kind
				);
				let stored = entry.value.clone().unwrap_or_default();
				let value = decode_value(entry.kind, &stored).unwrap_or_else(|e| {
					panic!("log {}, {:02x?} {seq}: {e}", segment.id, entry.key)
				});
				add_version(
					&mut found,
					&entry.key,
					seq,
					Version {
						kind: entry.kind,
						timestamp: entry.timestamp,
						value,
						from_log: true,
					},
				);
			}
		}
	}
	found
}

/// What the oracle would hold for a value: the length and the hash, or just the length for a
/// value that is in the value log, which this crate does not read.
fn digest(found: &mut Found, value: &Option<Value>) -> Option<(usize, Option<String>)> {
	let value = value.as_ref()?;
	if value.pointer {
		found.pointers += 1;
		Some((pointer_value_size(&value.payload), None))
	} else {
		Some((value.payload.len(), Some(fnv(&value.payload))))
	}
}

fn matches(found: &mut Found, got: Option<(usize, Option<String>)>, want: &Option<Digest>) -> bool {
	match (got, want) {
		(None, None) => true,
		(Some((len, Some(hash))), Some((want_len, want_hash))) => {
			len == *want_len && hash == *want_hash
		}
		(Some((len, None)), Some((want_len, _))) => {
			found.pointers_by_length += 1;
			len == *want_len
		}
		_ => false,
	}
}

/// The value of every key whose newest version is a write, as the engine would return it.
fn latest(found: &Found) -> BTreeMap<Vec<u8>, Option<Value>> {
	found
		.versions
		.iter()
		.map(|(key, versions)| {
			let (_, newest) = versions.iter().next_back().expect("a key has a version");
			let live = !matches!(newest.kind, Kind::Delete | Kind::SoftDelete);
			(key.clone(), live.then(|| newest.value.clone()).flatten())
		})
		.collect()
}

fn check(name: &str, db: &Path, oracle: &Path) -> Found {
	let mut found = read_database(db);
	let oracle = load_oracle(oracle);
	let latest = latest(&found);

	// The latest value of every key the oracle lists.
	assert!(!oracle.gets.is_empty(), "{name}: the oracle has keys");
	for (key, want) in &oracle.gets {
		let got = latest.get(key).cloned().flatten();
		let got = digest(&mut found, &got);
		assert!(
			matches(&mut found, got.clone(), want),
			"{name}: the latest value of {key:02x?} is {got:?}, the engine said {want:?}"
		);
	}

	// The latest scan is exactly the keys that have a value, with those values.
	let scanned: BTreeSet<&Vec<u8>> = oracle.scan.iter().map(|(key, _)| key).collect();
	let live: BTreeSet<&Vec<u8>> =
		latest.iter().filter(|(_, value)| value.is_some()).map(|(key, _)| key).collect();
	assert_eq!(live, scanned, "{name}: the keys of the latest scan");
	for (key, want) in &oracle.scan {
		let got = digest(&mut found, latest.get(key).unwrap());
		assert!(matches(&mut found, got, want), "{name}: the scanned value of {key:02x?}");
	}

	// Every version `history` listed is one that was read, with its timestamp, kind and value.
	// (A version that a compaction dropped is in neither.)
	for row in &oracle.history {
		let version = found
			.versions
			.get(&row.key)
			.and_then(|versions| versions.get(&row.seq))
			.cloned()
			.unwrap_or_else(|| {
				panic!("{name}: version {} of {:02x?} was not read", row.seq, row.key)
			});
		assert_eq!(format!("{:?}", version.kind), row.kind, "{name}: kind of {:02x?}", row.key);
		assert_eq!(
			version.timestamp, row.timestamp,
			"{name}: timestamp of {:02x?} at {}",
			row.key, row.seq
		);
		let got = digest(&mut found, &version.value);
		assert!(
			matches(&mut found, got.clone(), &row.value),
			"{name}: value of {:02x?} at {} is {got:?}, the engine said {:?}",
			row.key,
			row.seq,
			row.value
		);
	}
	found
}

fn repository_root() -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// `<root>/<scenario>/db` next to `<root>/<scenario>/oracle.tsv`, for every scenario there is.
fn scenarios(root: &Path) -> Vec<(String, PathBuf, PathBuf)> {
	let mut out = Vec::new();
	for entry in fs::read_dir(root).unwrap().flatten() {
		let dir = entry.path();
		if dir.join("db").is_dir() && dir.join("oracle.tsv").is_file() {
			out.push((
				entry.file_name().to_string_lossy().into_owned(),
				dir.join("db"),
				dir.join("oracle.tsv"),
			));
		}
	}
	out.sort();
	out
}

fn committed_fixtures() -> Vec<(String, PathBuf, PathBuf)> {
	let root = repository_root().join("tests/fixtures/legacy");
	let mut out = Vec::new();
	for version in fs::read_dir(&root).unwrap().flatten() {
		for (name, db, oracle) in scenarios(&version.path()) {
			out.push((format!("{}/{name}", version.file_name().to_string_lossy()), db, oracle));
		}
	}
	out.sort();
	out
}

#[test]
fn every_committed_database_reads_as_the_engine_that_wrote_it_read_it() {
	let fixtures = committed_fixtures();
	assert!(fixtures.len() >= 9, "the committed fixtures are there: {}", fixtures.len());
	let mut tables = 0;
	let mut log_entries = 0;
	for (name, db, oracle) in &fixtures {
		let found = check(name, db, oracle);
		eprintln!("{name}: {}", found.summary());
		assert!(found.damaged.is_empty(), "{name}: {:?}", found.damaged);
		tables += found.tables;
		log_entries += found.log_entries;
	}
	assert!(tables > 0 && log_entries > 0, "the fixtures cover tables and the log");
}

/// Larger databases that are not in the repository: every scenario in
/// `$SKV_LEGACY_FIXTURES`, which is the directory of one version of the generator's output.
/// Some of them were aborted in the middle of a write, so a damaged log segment is reported and
/// not an error.
#[test]
#[ignore = "needs SKV_LEGACY_FIXTURES=<dir of the generated databases of one version>"]
fn every_generated_database_reads_as_the_engine_that_wrote_it_read_it() {
	let root = PathBuf::from(
		std::env::var("SKV_LEGACY_FIXTURES").expect("SKV_LEGACY_FIXTURES is the directory to read"),
	);
	let scenarios = scenarios(&root);
	assert!(!scenarios.is_empty(), "{} holds no scenario", root.display());
	let mut pointers = 0;
	let mut by_length = 0;
	let mut failures = Vec::new();
	for (name, db, oracle) in &scenarios {
		let outcome = std::panic::catch_unwind(|| check(name, db, oracle));
		match outcome {
			Ok(found) => {
				eprintln!("{name}: {}", found.summary());
				pointers += found.pointers;
				by_length += found.pointers_by_length;
			}
			Err(_) => failures.push(name.clone()),
		}
	}
	eprintln!(
		"{} scenarios, {} values in the value log (of which {} were compared by length only)",
		scenarios.len(),
		pointers,
		by_length
	);
	assert!(failures.is_empty(), "these did not read as the engine read them: {failures:?}");
}

// The tests below take apart a copy of one database.

fn copy_dir(from: &Path, to: &Path) {
	fs::create_dir_all(to).unwrap();
	for entry in fs::read_dir(from).unwrap().flatten() {
		let target = to.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &target);
		} else {
			fs::copy(entry.path(), target).unwrap();
		}
	}
}

/// A copy of a committed database with both a table and a log.
fn copy_of(version: &str, scenario: &str) -> (tempfile::TempDir, Layout) {
	let source =
		repository_root().join("tests/fixtures/legacy").join(version).join(scenario).join("db");
	let dir = tempfile::tempdir().unwrap();
	copy_dir(&source, dir.path());
	let layout = Layout::new(dir.path());
	(dir, layout)
}

fn only_table(layout: &Layout) -> PathBuf {
	let mut tables: Vec<_> = fs::read_dir(layout.sstable_dir()).unwrap().flatten().collect();
	assert_eq!(tables.len(), 1);
	tables.pop().unwrap().path()
}

#[test]
fn the_footer_tells_lsmv1_from_anything_else() {
	for version in ["v0.21.4", "v0.20.1"] {
		let (_dir, layout) = copy_of(version, "tiny-wal-tail");
		let path = only_table(&layout);
		assert_eq!(table_layout(&path).unwrap(), TableLayout::Lsmv1);
		assert!(TableReader::open(&path).is_ok());

		// The format byte of the current format: no block is read to know.
		let mut bytes = fs::read(&path).unwrap();
		let footer = bytes.len() - 50;
		bytes[footer] = 2;
		fs::write(&path, &bytes).unwrap();
		assert_eq!(table_layout(&path).unwrap(), TableLayout::Other);
		assert!(matches!(
			TableReader::open(&path),
			Err(surrealkv_compat_v1::Error::Unsupported(_))
		));

		// A table whose blocks are sealed says so in the checksum type.
		bytes[footer] = 1;
		bytes[footer + 1] = 2;
		fs::write(&path, &bytes).unwrap();
		assert_eq!(table_layout(&path).unwrap(), TableLayout::Other);

		// Not a table at all.
		bytes[footer + 1] = 1;
		let last = bytes.len() - 1;
		bytes[last] ^= 0xff;
		fs::write(&path, &bytes).unwrap();
		assert!(matches!(table_layout(&path), Err(surrealkv_compat_v1::Error::Corrupted(_))));
		assert!(TableReader::open(&path).is_err());
		fs::write(&path, &bytes[..30]).unwrap();
		assert!(matches!(table_layout(&path), Err(surrealkv_compat_v1::Error::Corrupted(_))));
	}
}

#[test]
fn a_damaged_block_is_an_error_and_ends_the_entries() {
	let (_dir, layout) = copy_of("v0.21.4", "tiny-wal-tail");
	let path = only_table(&layout);
	let intact: Vec<_> = TableReader::open(&path).unwrap().entries().unwrap().collect();
	assert!(intact.iter().all(|entry| entry.is_ok()));
	assert!(intact.len() > 5);

	// A byte of the first data block, which the table opens past: only reading it finds out.
	let mut bytes = fs::read(&path).unwrap();
	bytes[3] ^= 0xff;
	fs::write(&path, &bytes).unwrap();
	let table = TableReader::open(&path).unwrap();
	let read: Vec<_> = table.entries().unwrap().collect();
	assert_eq!(read.len(), 1);
	assert!(matches!(read[0], Err(surrealkv_compat_v1::Error::Corrupted(_))));
}

#[test]
fn a_log_cut_in_the_middle_of_a_record_is_not_damaged_and_a_flipped_byte_is() {
	for version in ["v0.21.4", "v0.20.1"] {
		let (_dir, layout) = copy_of(version, "tiny-wal-tail");
		let segments = wal_segments(&layout.wal_dir()).unwrap();
		assert_eq!(segments.len(), 1);
		let path = &segments[0].path;
		let whole = read_wal_segment(path).unwrap();
		assert_eq!(whole.damage, None);
		assert!(whole.batches.len() > 1);

		let bytes = fs::read(path).unwrap();
		for cut in [bytes.len() - 1, bytes.len() - 5, bytes.len() / 2, 9, 3, 0] {
			fs::write(path, &bytes[..cut]).unwrap();
			let read = read_wal_segment(path).unwrap();
			assert_eq!(read.damage, None, "{version}: cut at {cut}");
			assert!(read.batches.len() < whole.batches.len(), "{version}: cut at {cut}");
			assert_eq!(read.batches[..], whole.batches[..read.batches.len()]);
		}

		let mut flipped = bytes.clone();
		let at = bytes.len() - 3;
		flipped[at] ^= 0xff;
		fs::write(path, &flipped).unwrap();
		let read = read_wal_segment(path).unwrap();
		assert!(read.damage.is_some(), "{version}: a flipped byte is damage");
		assert_eq!(read.batches.len(), whole.batches.len() - 1);
		assert_eq!(read.batches[..], whole.batches[..read.batches.len()]);
	}
}

#[test]
fn a_directory_without_a_manifest_or_with_another_version_is_not_lsmv1() {
	let (dir, layout) = copy_of("v0.21.4", "tiny-plain");
	assert!(is_lsmv1_dir(dir.path()));
	let mut manifest = fs::read(layout.manifest_path()).unwrap();
	manifest[1] = 2;
	fs::write(layout.manifest_path(), &manifest).unwrap();
	assert!(!is_lsmv1_dir(dir.path()));
	fs::remove_file(layout.manifest_path()).unwrap();
	assert!(!is_lsmv1_dir(dir.path()));
	assert!(!is_lsmv1_dir(&dir.path().join("nowhere")));
}
