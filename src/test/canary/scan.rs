//! The scanner: a walk over every file under a directory that reports where a marker occurs.
//!
//! What it is for. "Encrypted at rest" is only a claim until something can fail it. A test writes
//! markers, which are strings that nothing else in the database can contain, and looks for them
//! in every byte of every file that the database left. If the feature did nothing, the markers
//! would be there. The look must itself be trusted, which is why this file tests it: a marker
//! planted in each kind of place is found, one split by the scanner's own read boundary is found,
//! and a directory without one returns nothing.
//!
//! What it looks at. Every regular file at every depth, whatever its name: the temporary,
//! quarantine and backup names that the engine uses are not special to it, nor are names it has
//! never heard of. It never follows a symbolic link, so a link out of the tree cannot make a
//! marker that lives elsewhere look like one that was left here, and a link back into the tree
//! cannot make it loop. The files it looked at are returned with the findings, so that a test
//! that finds nothing can show that it had something to look at.
//!
//! The markers. A marker is three strings of twelve random printable bytes. A file is taken to
//! hold the marker when it holds any one of its three pieces, which is what makes it robust: a
//! block format that shortens a key against the one before it, or a codec that cuts a literal
//! where the data repeats, can take a piece apart, and it cannot do that to all three. The
//! pieces are drawn so that no four bytes occur in two markers: the shortest repeat that Snappy,
//! LZ4 or zstd will refer back to is four bytes, so no marker is ever cut by a reference to
//! another. The generator is seeded, and the seed is part of every message, so a run that fails
//! can be repeated with `SURREALKV_CANARY_SEED`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};

use rand::Rng;

/// The shortest needle the matcher takes: it finds its candidates by the first eight bytes.
pub(crate) const MIN_NEEDLE_LEN: usize = 8;

/// How much of a file one read asks for.
pub(crate) const DEFAULT_CHUNK: usize = 64 * 1024;

/// The matcher keeps one flag for each of this many bits of a hash of eight bytes, so that
/// almost every position of random data is discarded with one lookup.
const FILTER_BITS: u32 = 18;

// ---------------------------------------------------------------------------
// the matcher
// ---------------------------------------------------------------------------

/// A string to look for, and the group of strings it belongs to. A group is found when any of
/// its needles is.
#[derive(Clone, Debug)]
pub(crate) struct Needle {
	pub group: usize,
	pub label: String,
	pub bytes: Vec<u8>,
}

/// What to look for.
pub(crate) struct Needles {
	items: Vec<Needle>,
	filter: Vec<bool>,
	heads: HashMap<u64, Vec<usize>>,
	longest: usize,
}

fn head_of(bytes: &[u8]) -> u64 {
	let mut head = [0u8; MIN_NEEDLE_LEN];
	head.copy_from_slice(&bytes[..MIN_NEEDLE_LEN]);
	u64::from_be_bytes(head)
}

fn slot(head: u64) -> usize {
	(head.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - FILTER_BITS)) as usize
}

impl Needles {
	pub(crate) fn new(items: Vec<Needle>) -> Self {
		let mut filter = vec![false; 1 << FILTER_BITS];
		let mut heads: HashMap<u64, Vec<usize>> = HashMap::new();
		let mut longest = 0;
		for (at, needle) in items.iter().enumerate() {
			assert!(
				needle.bytes.len() >= MIN_NEEDLE_LEN,
				"the needle {} is shorter than {MIN_NEEDLE_LEN} bytes",
				needle.label
			);
			let head = head_of(&needle.bytes);
			filter[slot(head)] = true;
			heads.entry(head).or_default().push(at);
			longest = longest.max(needle.bytes.len());
		}
		Self {
			items,
			filter,
			heads,
			longest,
		}
	}

	pub(crate) fn len(&self) -> usize {
		self.items.len()
	}

	pub(crate) fn get(&self, at: usize) -> &Needle {
		&self.items[at]
	}

	/// Appends to `found` every match in `buf` that ends after its first `searched` bytes, with
	/// its offset taken from `base`, the offset of `buf[0]` in the stream. The bytes before
	/// `searched` were part of the buffer that was searched before, and a match that lies wholly
	/// in them was reported then.
	fn search(&self, buf: &[u8], searched: usize, base: u64, found: &mut Vec<(u64, usize)>) {
		let mut window = 0u64;
		for (at, &byte) in buf.iter().enumerate() {
			window = (window << 8) | u64::from(byte);
			if at + 1 < MIN_NEEDLE_LEN || !self.filter[slot(window)] {
				continue;
			}
			let Some(candidates) = self.heads.get(&window) else {
				continue;
			};
			let start = at + 1 - MIN_NEEDLE_LEN;
			for &needle in candidates {
				let bytes = &self.items[needle].bytes;
				let end = start + bytes.len();
				if end <= buf.len() && end > searched && buf[start..end] == bytes[..] {
					found.push((base + start as u64, needle));
				}
			}
		}
	}
}

/// Fills `buf` from `reader` as far as it goes: a short read is not the end, only a read of zero
/// bytes is.
fn read_up_to<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<usize> {
	let mut filled = 0;
	while filled < buf.len() {
		match reader.read(&mut buf[filled..]) {
			Ok(0) => break,
			Ok(read) => filled += read,
			Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
			Err(e) => return Err(e),
		}
	}
	Ok(filled)
}

/// Reads `reader` to its end, `chunk` bytes at a time, and returns how many bytes it held and
/// every match as the offset where it starts and the index of the needle. A match that a read
/// boundary falls in the middle of is found once, because the last bytes of one read, as many as
/// the longest needle needs less one, are searched again with the next.
pub(crate) fn scan_reader<R: Read>(
	mut reader: R,
	needles: &Needles,
	chunk: usize,
) -> io::Result<(u64, Vec<(u64, usize)>)> {
	assert!(chunk > 0, "a read of no bytes never ends");
	let carry = needles.longest.saturating_sub(1);
	let mut buf: Vec<u8> = Vec::new();
	let mut base = 0u64;
	let mut searched = 0usize;
	let mut total = 0u64;
	let mut found = Vec::new();
	loop {
		let old = buf.len();
		buf.resize(old + chunk, 0);
		let read = read_up_to(&mut reader, &mut buf[old..])?;
		buf.truncate(old + read);
		if read == 0 {
			break;
		}
		total += read as u64;
		needles.search(&buf, searched, base, &mut found);
		let keep = carry.min(buf.len());
		let dropped = buf.len() - keep;
		buf.drain(..dropped);
		base += dropped as u64;
		searched = keep;
	}
	Ok((total, found))
}

/// The matches in `bytes`, as offset and needle index.
pub(crate) fn scan_bytes(bytes: &[u8], needles: &Needles) -> Vec<(u64, usize)> {
	scan_reader(Cursor::new(bytes), needles, bytes.len().max(1))
		.expect("a read of a slice does not fail")
		.1
}

// ---------------------------------------------------------------------------
// the walk
// ---------------------------------------------------------------------------

/// A file the scan read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScannedFile {
	/// The path, relative to the directory that was scanned.
	pub path: PathBuf,
	/// How many bytes were searched: the length of the file, or of what a view of it showed.
	pub bytes: u64,
}

/// What the scan left alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Skipped {
	/// A symbolic link, which is never followed.
	Link(PathBuf),
	/// Something that is neither a file nor a directory: a pipe, a socket, a device.
	Special(PathBuf),
	/// A file or a directory that was listed and was gone when the scan came to it, as a file
	/// that a cleanup removes can be.
	Vanished(PathBuf),
}

/// One place where a needle was found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Finding {
	/// The file, relative to the directory that was scanned.
	pub file: PathBuf,
	/// Where in the file the needle starts. For a file that was scanned through a view, where in
	/// what the view showed.
	pub offset: u64,
	/// The index of the needle.
	pub needle: usize,
}

/// Everything a scan looked at and found.
#[derive(Clone, Debug, Default)]
pub(crate) struct Scan {
	pub files: Vec<ScannedFile>,
	pub skipped: Vec<Skipped>,
	pub findings: Vec<Finding>,
}

impl Scan {
	/// The groups of which at least one needle was found.
	pub(crate) fn groups(&self, needles: &Needles) -> BTreeSet<usize> {
		self.findings.iter().map(|finding| needles.get(finding.needle).group).collect()
	}

	/// The groups found in a file under one of the directories `places`, which are relative to
	/// the directory that was scanned.
	pub(crate) fn groups_in(&self, needles: &Needles, places: &[&str]) -> BTreeSet<usize> {
		self.findings
			.iter()
			.filter(|finding| places.iter().any(|place| finding.file.starts_with(place)))
			.map(|finding| needles.get(finding.needle).group)
			.collect()
	}

	/// The directories, of those in `places`, in which a finding is.
	pub(crate) fn places_with_findings<'a>(&self, places: &[&'a str]) -> BTreeSet<&'a str> {
		places
			.iter()
			.copied()
			.filter(|place| self.findings.iter().any(|finding| finding.file.starts_with(place)))
			.collect()
	}

	/// How many files under `place` the scan looked at.
	pub(crate) fn files_in(&self, place: &str) -> usize {
		self.files.iter().filter(|file| file.path.starts_with(place)).count()
	}

	/// How many bytes the scan searched.
	pub(crate) fn bytes(&self) -> u64 {
		self.files.iter().map(|file| file.bytes).sum()
	}

	/// The findings, a few of them, for the message of a failed assertion.
	pub(crate) fn describe(&self, needles: &Needles) -> String {
		let mut text = format!("{} finding(s)", self.findings.len());
		for finding in self.findings.iter().take(6) {
			text.push_str(&format!(
				"; {} at {} in {}",
				needles.get(finding.needle).label,
				finding.offset,
				finding.file.display()
			));
		}
		text
	}
}

/// What `scan_dir_with` shows of a file instead of its bytes, if it shows anything: the contents
/// of a table, say, decoded from the blocks that hold them. It is given the full path.
pub(crate) type View<'a> = &'a dyn Fn(&Path) -> Option<Vec<u8>>;

/// Scans every file under `root` for `needles`.
pub(crate) fn scan_dir(root: &Path, needles: &Needles) -> io::Result<Scan> {
	scan_dir_with(root, needles, DEFAULT_CHUNK, &|_| None)
}

/// Scans every file under `root`, reading `chunk` bytes at a time, and the view of a file where
/// `view` gives one in place of its bytes.
pub(crate) fn scan_dir_with(
	root: &Path,
	needles: &Needles,
	chunk: usize,
	view: View<'_>,
) -> io::Result<Scan> {
	let mut scan = Scan::default();
	visit(root, Path::new(""), needles, chunk, view, &mut scan)?;
	Ok(scan)
}

fn visit(
	dir: &Path,
	relative: &Path,
	needles: &Needles,
	chunk: usize,
	view: View<'_>,
	scan: &mut Scan,
) -> io::Result<()> {
	let listing = match fs::read_dir(dir) {
		Ok(listing) => listing,
		Err(e) if e.kind() == io::ErrorKind::NotFound && !relative.as_os_str().is_empty() => {
			scan.skipped.push(Skipped::Vanished(relative.to_path_buf()));
			return Ok(());
		}
		Err(e) => return Err(e),
	};
	let mut entries = Vec::new();
	for entry in listing {
		match entry {
			Ok(entry) => entries.push(entry),
			Err(e) if e.kind() == io::ErrorKind::NotFound => {}
			Err(e) => return Err(e),
		}
	}
	entries.sort_by_key(fs::DirEntry::file_name);
	for entry in entries {
		let path = entry.path();
		let relative = relative.join(entry.file_name());
		// The type of the entry itself: a link is a link, whatever it points at.
		let kind = match entry.file_type() {
			Ok(kind) => kind,
			Err(e) if e.kind() == io::ErrorKind::NotFound => {
				scan.skipped.push(Skipped::Vanished(relative));
				continue;
			}
			Err(e) => return Err(e),
		};
		if kind.is_symlink() {
			scan.skipped.push(Skipped::Link(relative));
		} else if kind.is_dir() {
			visit(&path, &relative, needles, chunk, view, scan)?;
		} else if kind.is_file() {
			scan_file(&path, relative, needles, chunk, view, scan)?;
		} else {
			scan.skipped.push(Skipped::Special(relative));
		}
	}
	Ok(())
}

/// Scans the file at `path`, which is `relative` to the directory that is scanned. A file that
/// is not there is recorded as gone, and is not an error.
pub(crate) fn scan_file(
	path: &Path,
	relative: PathBuf,
	needles: &Needles,
	chunk: usize,
	view: View<'_>,
	scan: &mut Scan,
) -> io::Result<()> {
	let scanned = match view(path) {
		Some(shown) => scan_reader(Cursor::new(shown), needles, chunk),
		None => match fs::File::open(path) {
			Ok(file) => scan_reader(file, needles, chunk),
			Err(e) if e.kind() == io::ErrorKind::NotFound => {
				scan.skipped.push(Skipped::Vanished(relative));
				return Ok(());
			}
			Err(e) => return Err(e),
		},
	};
	let (bytes, found) = scanned?;
	for (offset, needle) in found {
		scan.findings.push(Finding {
			file: relative.clone(),
			offset,
			needle,
		});
	}
	scan.files.push(ScannedFile {
		path: relative,
		bytes,
	});
	Ok(())
}

// ---------------------------------------------------------------------------
// the markers
// ---------------------------------------------------------------------------

/// The number of pieces of a marker, and the length of each.
pub(crate) const PIECES: usize = 3;
pub(crate) const PIECE_LEN: usize = 12;

const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// The variable that fixes the seed of a run.
pub(crate) const SEED_VARIABLE: &str = "SURREALKV_CANARY_SEED";

/// A string that nothing but the test that made it can have written: three pieces of twelve
/// printable bytes, drawn at random.
#[derive(Clone, Debug)]
pub(crate) struct Marker {
	pub label: String,
	pub pieces: [[u8; PIECE_LEN]; PIECES],
}

impl Marker {
	/// The whole marker, which is what a test writes into a key or a value.
	pub(crate) fn text(&self) -> Vec<u8> {
		self.pieces.concat()
	}
}

/// A source of markers.
pub(crate) struct Markers {
	seed: u64,
	state: u64,
	list: Vec<Marker>,
	grams: HashSet<[u8; 4]>,
}

impl Markers {
	pub(crate) fn new(seed: u64) -> Self {
		Self {
			seed,
			state: seed,
			list: Vec::new(),
			grams: HashSet::new(),
		}
	}

	/// A source seeded from `SURREALKV_CANARY_SEED` if it is set, and from the system otherwise.
	pub(crate) fn from_environment() -> Self {
		let seed = std::env::var(SEED_VARIABLE)
			.ok()
			.and_then(|value| value.trim().parse::<u64>().ok())
			.unwrap_or_else(|| rand::rng().random::<u64>());
		Self::new(seed)
	}

	pub(crate) fn seed(&self) -> u64 {
		self.seed
	}

	pub(crate) fn len(&self) -> usize {
		self.list.len()
	}

	pub(crate) fn get(&self, at: usize) -> &Marker {
		&self.list[at]
	}

	/// The next 64 bits of the generator, which is SplitMix64: a few lines, no dependency, and
	/// the same sequence on every platform for one seed.
	fn next(&mut self) -> u64 {
		self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
		let mut z = self.state;
		z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
		z ^ (z >> 31)
	}

	/// Draws a marker that shares no four bytes with one drawn before, nor with itself, and
	/// returns its index.
	pub(crate) fn mint(&mut self, label: impl Into<String>) -> usize {
		for _ in 0..10_000 {
			let mut text = [0u8; PIECES * PIECE_LEN];
			for byte in &mut text {
				*byte = ALPHABET[(self.next() % ALPHABET.len() as u64) as usize];
			}
			let grams: Vec<[u8; 4]> =
				text.windows(4).map(|gram| [gram[0], gram[1], gram[2], gram[3]]).collect();
			let own: HashSet<[u8; 4]> = grams.iter().copied().collect();
			if own.len() != grams.len() || grams.iter().any(|gram| self.grams.contains(gram)) {
				continue;
			}
			self.grams.extend(grams);
			let mut pieces = [[0u8; PIECE_LEN]; PIECES];
			for (at, piece) in pieces.iter_mut().enumerate() {
				piece.copy_from_slice(&text[at * PIECE_LEN..(at + 1) * PIECE_LEN]);
			}
			self.list.push(Marker {
				label: label.into(),
				pieces,
			});
			return self.list.len() - 1;
		}
		panic!("no marker was drawn in ten thousand tries (seed {})", self.seed);
	}

	/// The needles of every marker drawn so far: the group of a needle is the index of its
	/// marker.
	pub(crate) fn needles(&self) -> Needles {
		self.needles_with(&[])
	}

	/// The needles of every marker, and then each of `extra`, which is a label and bytes that are
	/// no marker and that no file may hold either, such as the root key of a database. Each is a
	/// group of its own, numbered after the markers.
	pub(crate) fn needles_with(&self, extra: &[(&str, &[u8])]) -> Needles {
		let mut items: Vec<Needle> = self
			.list
			.iter()
			.enumerate()
			.flat_map(|(group, marker)| {
				marker.pieces.iter().enumerate().map(move |(at, piece)| Needle {
					group,
					label: format!("{}#{at}", marker.label),
					bytes: piece.to_vec(),
				})
			})
			.collect();
		for (at, (label, bytes)) in extra.iter().enumerate() {
			items.push(Needle {
				group: self.list.len() + at,
				label: (*label).to_string(),
				bytes: bytes.to_vec(),
			});
		}
		Needles::new(items)
	}
}

// ---------------------------------------------------------------------------
// tests of the scanner
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
	use tempfile::TempDir;

	use super::*;

	fn one_marker(label: &str) -> (Markers, Needles) {
		let mut markers = Markers::new(11);
		markers.mint(label);
		let needles = markers.needles();
		(markers, needles)
	}

	/// `bytes` with `plant` at `offset`, over filler that no marker is made of.
	fn planted(len: usize, plants: &[(usize, &[u8])]) -> Vec<u8> {
		let mut bytes = vec![b'.'; len];
		for (offset, plant) in plants {
			bytes[*offset..*offset + plant.len()].copy_from_slice(plant);
		}
		bytes
	}

	#[test]
	fn a_marker_planted_in_each_kind_of_place_is_found_where_it_was_put() {
		let (markers, needles) = one_marker("planted");
		let marker = markers.get(0).text();
		let root = TempDir::new().unwrap();

		// The places that the engine keeps files in, with the names that it gives the ones that
		// are not the database: a temporary, a quarantine, a repair, a backup, a restore marker.
		let places = [
			"top",
			"wal/00000000000000000003.wal",
			"wal/00000000000000000003.wal.corrupt",
			"wal/00000000000000000003.wal.corrupt.1",
			"wal/repair_temp/00000000000000000000.wal",
			"manifest/.tmp_1700000000_4711",
			"vlog/00000000000000000002.vlog.torn",
			"sstables/00000000000000000009.sst",
			"_rocksdb_backup/CURRENT",
			"_surrealkv_v0_backup/deep/er/and/deeper/000001.sst",
			"RESTORE_IN_PROGRESS",
			"no-extension",
			".hidden",
			"a name with spaces.tmp",
		];
		// Each file has its marker at an offset of its own, after some bytes that are not one,
		// and is longer than that by a length of its own.
		let offset_of = |at: usize| 5 + at;
		let len_of = |at: usize| 60 + 3 * at;
		for (at, place) in places.iter().enumerate() {
			let path = root.path().join(place);
			fs::create_dir_all(path.parent().unwrap()).unwrap();
			fs::write(&path, planted(len_of(at), &[(offset_of(at), marker.as_slice())])).unwrap();
		}
		// And a file with nothing in it, and one that holds the marker twice.
		fs::write(root.path().join("empty"), b"").unwrap();
		let twice = planted(200, &[(7, marker.as_slice()), (150, marker.as_slice())]);
		fs::write(root.path().join("twice"), twice).unwrap();

		let scan = scan_dir(root.path(), &needles).unwrap();

		// Every needle that was planted: the three pieces of the marker are each found at the
		// offset of the marker and 12 and 24 bytes after it.
		let mut expected: Vec<(String, u64, usize)> = Vec::new();
		for (at, place) in places.iter().enumerate() {
			for piece in 0..PIECES {
				expected.push((
					place.to_string(),
					(offset_of(at) + piece * PIECE_LEN) as u64,
					piece,
				));
			}
		}
		for piece in 0..PIECES {
			expected.push(("twice".to_string(), (7 + piece * PIECE_LEN) as u64, piece));
			expected.push(("twice".to_string(), (150 + piece * PIECE_LEN) as u64, piece));
		}
		expected.sort();
		let mut found: Vec<(String, u64, usize)> = scan
			.findings
			.iter()
			.map(|finding| {
				(finding.file.to_string_lossy().replace('\\', "/"), finding.offset, finding.needle)
			})
			.collect();
		found.sort();
		assert_eq!(found, expected);

		// The files it looked at are all of them, the empty one included, with their lengths.
		assert_eq!(scan.files.len(), places.len() + 2);
		assert!(scan.skipped.is_empty());
		let empty = scan.files.iter().find(|file| file.path == Path::new("empty")).unwrap();
		assert_eq!(empty.bytes, 0);
		let deeper = places.iter().position(|place| place.ends_with("deeper/000001.sst")).unwrap();
		let deep = scan
			.files
			.iter()
			.find(|file| file.path.ends_with("deeper/000001.sst"))
			.expect("the file five directories down was looked at");
		assert_eq!(deep.bytes, len_of(deeper) as u64);
		assert_eq!(scan.groups(&needles), BTreeSet::from([0]));
		assert_eq!(
			scan.groups_in(&needles, &["wal"]),
			BTreeSet::from([0]),
			"the findings under a directory are the ones in a file below it"
		);
		assert!(
			scan.groups_in(&needles, &["vlog/0000"]).is_empty(),
			"a place is a path and not the start of a name"
		);
		assert_eq!(
			scan.places_with_findings(&["wal", "sstables", "vlog", "nothing"]),
			BTreeSet::from(["wal", "sstables", "vlog"])
		);
		assert_eq!(scan.files_in("wal"), 4);
	}

	#[test]
	fn a_directory_without_a_marker_returns_nothing_and_says_what_it_looked_at() {
		let (markers, needles) = one_marker("absent");
		let marker = markers.get(0).text();
		let root = TempDir::new().unwrap();

		// Nothing at all.
		let scan = scan_dir(root.path(), &needles).unwrap();
		assert!(scan.findings.is_empty() && scan.files.is_empty() && scan.skipped.is_empty());

		// Files that come close: the marker with one byte of it changed, in each position, and
		// with its last byte missing, and the pieces of it in another order.
		fs::create_dir_all(root.path().join("a/b")).unwrap();
		for at in 0..marker.len() {
			let mut near = marker.clone();
			near[at] = if near[at] == b'!' {
				b'?'
			} else {
				b'!'
			};
			// The changed byte falls in one piece, and the other two are whole: so those are found,
			// which is what a piece is for, and the file with every piece changed is not.
			fs::write(root.path().join("a").join(format!("near-{at}")), &near).unwrap();
		}
		let mut every_piece = marker.clone();
		for piece in 0..PIECES {
			every_piece[piece * PIECE_LEN] = b'!';
		}
		fs::write(root.path().join("a/b/every-piece-changed"), &every_piece).unwrap();
		fs::write(root.path().join("short"), &marker[..PIECE_LEN - 1]).unwrap();
		let scan = scan_dir(root.path(), &needles).unwrap();
		assert_eq!(scan.files.len(), marker.len() + 2);
		for finding in &scan.findings {
			assert!(finding.file.starts_with("a"), "{finding:?}");
			assert!(!finding.file.starts_with("a/b"), "{finding:?}");
		}
		assert!(scan.findings.iter().all(|finding| finding.file != Path::new("short")));
		assert_eq!(
			scan.findings.len(),
			marker.len() * (PIECES - 1),
			"each file of the first group holds the two pieces that were not changed"
		);

		// Random bytes of a size that gives the filter its chances.
		let mut state = 0x1234_5678_9ABC_DEF0u64;
		let noise: Vec<u8> = (0..1 << 20)
			.map(|_| {
				state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
				(state >> 56) as u8
			})
			.collect();
		let clean = TempDir::new().unwrap();
		fs::write(clean.path().join("noise"), &noise).unwrap();
		let scan = scan_dir(clean.path(), &needles).unwrap();
		assert!(scan.findings.is_empty(), "{}", scan.describe(&needles));
		assert_eq!(scan.bytes(), 1 << 20);
	}

	#[test]
	fn a_marker_that_a_read_boundary_falls_in_is_found_once_at_every_split() {
		let (markers, needles) = one_marker("split");
		let marker = markers.get(0).text();
		let piece = &markers.get(0).pieces[1];

		for chunk in [1usize, 2, 7, 8, 9, 11, 12, 35, 36, 37, 64] {
			// The whole marker at every offset that moves it across the first three boundaries.
			for offset in 0..=3 * chunk + 2 {
				let bytes = planted(offset + marker.len() + 9, &[(offset, marker.as_slice())]);
				let (total, found) = scan_reader(Cursor::new(&bytes), &needles, chunk).unwrap();
				assert_eq!(total, bytes.len() as u64);
				let mut offsets: Vec<(u64, usize)> = found;
				offsets.sort_unstable();
				assert_eq!(
					offsets,
					(0..PIECES).map(|p| ((offset + p * PIECE_LEN) as u64, p)).collect::<Vec<_>>(),
					"chunk {chunk}, offset {offset}"
				);
			}
			// A single piece at the end of the stream, and one at the very start.
			let bytes = planted(chunk + 40, &[(chunk + 40 - piece.len(), &piece[..])]);
			let (_, found) = scan_reader(Cursor::new(&bytes), &needles, chunk).unwrap();
			assert_eq!(found, vec![((chunk + 40 - piece.len()) as u64, 1)], "chunk {chunk}");
			let (_, found) = scan_reader(Cursor::new(piece.to_vec()), &needles, chunk).unwrap();
			assert_eq!(found, vec![(0, 1)], "chunk {chunk}");
		}
	}

	/// A reader that never gives more than a few bytes at a time and is interrupted now and then,
	/// as a pipe or a loaded file system is.
	struct Trickle<'a> {
		bytes: &'a [u8],
		at: usize,
		calls: usize,
	}

	impl Read for Trickle<'_> {
		fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
			self.calls += 1;
			if self.calls & 3 == 3 {
				return Err(io::Error::from(io::ErrorKind::Interrupted));
			}
			let n = buf.len().min(3).min(self.bytes.len() - self.at);
			buf[..n].copy_from_slice(&self.bytes[self.at..self.at + n]);
			self.at += n;
			Ok(n)
		}
	}

	#[test]
	fn short_and_interrupted_reads_do_not_end_a_scan_or_hide_a_marker() {
		let (markers, needles) = one_marker("trickle");
		let marker = markers.get(0).text();
		let at_end = 300 - marker.len();
		let bytes = planted(
			300,
			&[(0, marker.as_slice()), (101, marker.as_slice()), (at_end, marker.as_slice())],
		);
		let (total, found) = scan_reader(
			Trickle {
				bytes: &bytes,
				at: 0,
				calls: 0,
			},
			&needles,
			50,
		)
		.unwrap();
		assert_eq!(total, 300);
		assert_eq!(found.len(), 3 * PIECES);
		let starts: BTreeSet<u64> = found.iter().map(|(offset, _)| *offset).collect();
		assert!(starts.contains(&0) && starts.contains(&101) && starts.contains(&(at_end as u64)));
	}

	#[test]
	fn needles_that_start_alike_or_overlap_or_repeat_are_all_found() {
		let items = vec![
			Needle {
				group: 0,
				label: "short".into(),
				bytes: b"abcdefgh12".to_vec(),
			},
			Needle {
				group: 1,
				label: "long".into(),
				bytes: b"abcdefgh12345678".to_vec(),
			},
			Needle {
				group: 2,
				label: "overlaps".into(),
				bytes: b"12345678wxyz".to_vec(),
			},
			Needle {
				group: 3,
				label: "same as the first".into(),
				bytes: b"abcdefgh12".to_vec(),
			},
		];
		let needles = Needles::new(items);
		let found = scan_bytes(b"..abcdefgh12345678wxyz..abcdefgh12.", &needles);
		let mut found: Vec<(u64, usize)> = found;
		found.sort_unstable();
		assert_eq!(found, vec![(2, 0), (2, 1), (2, 3), (10, 2), (24, 0), (24, 3)]);

		// With the stream cut anywhere, the same.
		for chunk in 1..40 {
			let (_, mut cut) = scan_reader(
				Cursor::new(&b"..abcdefgh12345678wxyz..abcdefgh12."[..]),
				&needles,
				chunk,
			)
			.unwrap();
			cut.sort_unstable();
			assert_eq!(cut, found, "chunk {chunk}");
		}
		assert!(scan_bytes(b"", &needles).is_empty());
		assert!(scan_bytes(b"abcdefgh1", &needles).is_empty());
	}

	#[test]
	fn a_large_file_is_read_in_many_chunks_and_a_marker_anywhere_in_it_is_found() {
		let (markers, needles) = one_marker("large");
		let marker = markers.get(0).text();
		const LEN: usize = 8 << 20;

		// Pseudo-random contents, with the marker at the start, across a boundary of the default
		// read, in the middle, and ending at the last byte.
		let mut state = 42u64;
		let mut bytes: Vec<u8> = (0..LEN)
			.map(|_| {
				state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
				(state >> 56) as u8
			})
			.collect();
		let offsets =
			[0, DEFAULT_CHUNK - 5, 5 * DEFAULT_CHUNK + 20, LEN / 2 + 1, LEN - marker.len()];
		for offset in offsets {
			bytes[offset..offset + marker.len()].copy_from_slice(&marker);
		}
		let root = TempDir::new().unwrap();
		fs::create_dir_all(root.path().join("vlog")).unwrap();
		fs::write(root.path().join("vlog/00000000000000000001.vlog"), &bytes).unwrap();

		let scan = scan_dir(root.path(), &needles).unwrap();
		assert_eq!(scan.files.len(), 1);
		assert_eq!(scan.files[0].bytes, LEN as u64);
		let mut starts: Vec<u64> = scan
			.findings
			.iter()
			.filter(|finding| finding.needle == 0)
			.map(|finding| finding.offset)
			.collect();
		starts.sort_unstable();
		assert_eq!(starts, offsets.iter().map(|&offset| offset as u64).collect::<Vec<_>>());
		assert_eq!(scan.findings.len(), offsets.len() * PIECES);
	}

	#[test]
	fn a_file_that_a_view_shows_differently_is_scanned_as_the_view_shows_it() {
		let (markers, needles) = one_marker("viewed");
		let marker = markers.get(0).text();
		let root = TempDir::new().unwrap();
		fs::write(root.path().join("coded.bin"), b"nothing of it can be read in these bytes")
			.unwrap();
		fs::write(root.path().join("plain.bin"), planted(60, &[(10, marker.as_slice())])).unwrap();

		let shown = planted(100, &[(30, marker.as_slice())]);
		let view = |path: &Path| {
			path.file_name().is_some_and(|name| name == "coded.bin").then(|| shown.clone())
		};
		let scan = scan_dir_with(root.path(), &needles, 16, &view).unwrap();
		let mut found: Vec<(String, u64)> = scan
			.findings
			.iter()
			.filter(|finding| finding.needle == 0)
			.map(|finding| (finding.file.to_string_lossy().into_owned(), finding.offset))
			.collect();
		found.sort();
		assert_eq!(found, vec![("coded.bin".to_string(), 30), ("plain.bin".to_string(), 10)]);
		let coded = scan.files.iter().find(|file| file.path == Path::new("coded.bin")).unwrap();
		assert_eq!(coded.bytes, 100, "what the view showed is what was searched");
	}

	#[test]
	fn a_file_that_is_gone_when_the_scan_comes_to_it_is_recorded_and_the_scan_goes_on() {
		let (markers, needles) = one_marker("gone");
		let marker = markers.get(0).text();
		let root = TempDir::new().unwrap();
		fs::write(root.path().join("there"), planted(50, &[(3, marker.as_slice())])).unwrap();

		let mut scan = Scan::default();
		scan_file(
			&root.path().join("removed-by-a-cleanup"),
			PathBuf::from("vlog/removed-by-a-cleanup"),
			&needles,
			DEFAULT_CHUNK,
			&|_| None,
			&mut scan,
		)
		.unwrap();
		assert_eq!(
			scan.skipped,
			vec![Skipped::Vanished(PathBuf::from("vlog/removed-by-a-cleanup"))]
		);
		assert!(scan.files.is_empty() && scan.findings.is_empty());

		// An entry that is not there is the same for the walk; a root that is not there is an
		// error, since a scan of nothing that finds nothing says nothing.
		let whole = scan_dir(root.path(), &needles).unwrap();
		assert_eq!(whole.findings.len(), PIECES);
		assert!(scan_dir(&root.path().join("no-such-directory"), &needles).is_err());
	}

	#[cfg(unix)]
	#[test]
	fn a_symbolic_link_is_not_followed_out_of_the_tree_or_round_in_a_circle() {
		use std::os::unix::fs::symlink;

		let (markers, needles) = one_marker("linked");
		let marker = markers.get(0).text();
		let outside = TempDir::new().unwrap();
		fs::write(outside.path().join("elsewhere"), planted(40, &[(0, marker.as_slice())]))
			.unwrap();
		fs::create_dir_all(outside.path().join("dir")).unwrap();
		fs::write(outside.path().join("dir/file"), planted(40, &[(0, marker.as_slice())])).unwrap();

		let root = TempDir::new().unwrap();
		fs::create_dir_all(root.path().join("sstables")).unwrap();
		fs::write(root.path().join("sstables/real"), b"no marker here, only filler bytes").unwrap();
		symlink(outside.path().join("elsewhere"), root.path().join("link-to-a-file")).unwrap();
		symlink(outside.path().join("dir"), root.path().join("link-to-a-directory")).unwrap();
		symlink(root.path(), root.path().join("sstables/link-to-the-root")).unwrap();
		symlink(root.path().join("no-such-target"), root.path().join("dangling")).unwrap();

		let scan = scan_dir(root.path(), &needles).unwrap();
		assert!(scan.findings.is_empty(), "{}", scan.describe(&needles));
		assert_eq!(scan.files.len(), 1);
		let mut links: Vec<PathBuf> = scan
			.skipped
			.iter()
			.map(|skipped| match skipped {
				Skipped::Link(path) => path.clone(),
				other => panic!("{other:?}"),
			})
			.collect();
		links.sort();
		assert_eq!(
			links,
			vec![
				PathBuf::from("dangling"),
				PathBuf::from("link-to-a-directory"),
				PathBuf::from("link-to-a-file"),
				PathBuf::from("sstables/link-to-the-root"),
			]
		);
	}

	#[test]
	fn markers_are_random_printable_and_share_no_four_bytes() {
		let mut markers = Markers::new(2024);
		for at in 0..300 {
			assert_eq!(markers.mint(format!("m{at}")), at);
		}
		assert_eq!(markers.len(), 300);

		let mut grams = HashSet::new();
		let mut pieces = HashSet::new();
		for at in 0..markers.len() {
			let marker = markers.get(at);
			let text = marker.text();
			assert_eq!(text.len(), PIECES * PIECE_LEN);
			assert!(text.iter().all(u8::is_ascii_alphanumeric), "{marker:?}");
			for piece in &marker.pieces {
				assert!(pieces.insert(*piece), "a piece was drawn twice");
			}
			for gram in text.windows(4) {
				assert!(grams.insert(gram.to_vec()), "four bytes occur twice: {gram:?}");
			}
		}

		// The same seed gives the same markers, another seed gives others.
		let mut again = Markers::new(2024);
		let mut other = Markers::new(2025);
		for at in 0..10 {
			again.mint("again");
			other.mint("other");
			assert_eq!(again.get(at).text(), markers.get(at).text());
			assert_ne!(other.get(at).text(), markers.get(at).text());
		}

		// A marker is a group of three needles, found when any one of them is.
		let needles = markers.needles();
		assert_eq!(needles.len(), 300 * PIECES);
		assert_eq!(needles.get(7 * PIECES + 2).group, 7);
		assert_eq!(needles.get(7 * PIECES + 2).label, "m7#2");
		assert_eq!(needles.get(7 * PIECES + 2).bytes, markers.get(7).pieces[2].to_vec());
		let mut stream = vec![b'.'; 40];
		stream[10..22].copy_from_slice(&markers.get(7).pieces[2]);
		let found = scan_bytes(&stream, &needles);
		assert_eq!(found, vec![(10, 7 * PIECES + 2)]);
	}

	#[test]
	fn a_key_that_is_not_text_is_found_like_a_marker_and_has_a_group_of_its_own() {
		let mut markers = Markers::new(5);
		markers.mint("one");
		markers.mint("two");
		// Bytes that no text has, and that a file would hold if a key were written to it.
		let key: [u8; 32] = std::array::from_fn(|at| (at as u8).wrapping_mul(37).wrapping_add(200));
		let needles = markers.needles_with(&[("the key", &key[..])]);
		assert_eq!(needles.len(), 2 * PIECES + 1);
		let last = needles.get(2 * PIECES);
		assert_eq!(
			(last.group, last.label.as_str(), last.bytes.as_slice()),
			(2, "the key", &key[..])
		);

		let mut stream = vec![0u8; 100];
		stream[17..49].copy_from_slice(&key);
		stream[60..96].copy_from_slice(&markers.get(1).text());
		for chunk in [1usize, 5, 31, 32, 33, 100] {
			let (_, mut found) = scan_reader(Cursor::new(&stream), &needles, chunk).unwrap();
			found.sort_unstable();
			assert_eq!(
				found,
				vec![(17, 2 * PIECES), (60, PIECES), (72, PIECES + 1), (84, PIECES + 2)],
				"chunk {chunk}"
			);
		}
		// A key that is in no file, and a half of it that is, are not found.
		assert!(scan_bytes(&stream[..40], &needles).is_empty());
	}

	#[test]
	fn the_seed_of_a_run_is_kept_so_that_a_run_can_be_repeated() {
		// Whether it came from the environment or from the system, a source says its seed, and
		// a source made from that seed draws the same markers.
		let mut first = Markers::from_environment();
		let mut again = Markers::new(first.seed());
		first.mint("a");
		again.mint("a");
		assert_eq!(first.get(0).text(), again.get(0).text());
		assert!(SEED_VARIABLE.starts_with("SURREALKV_"));
	}
}
