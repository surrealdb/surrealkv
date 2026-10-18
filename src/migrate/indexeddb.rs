//! The move of the rows of a legacy IndexedDB database into a database in the origin private file
//! system of a browser.
//!
//! The legacy store is the `kv` object store that SurrealDB's `indxdb://` backend wrote. It is read
//! and never changed, created or deleted. Its rows are copied into the database that the caller
//! names, which this module makes with a `Tree` of its own, and the copy is checked against the
//! source before it counts.
//!
//! The reading of the source is behind [`Source`] and the mounting of the files behind [`Mounter`],
//! so that all of this runs and is tested on a native build, over the memory file system with its
//! simulated crashes. The browser's implementations of the two are in [`browser`] (WebAssembly
//! only), and [`migrate_from_indexeddb`] is what an application calls.
//!
//! # How it is made safe
//!
//! A database that is half copied must never be taken for a database. The copy therefore starts
//! with a marker: a file at the root of the pool, named [`MARKER`], which is made durable before
//! any other file of the database exists, and removed, durably, as the last step of a copy that was
//! verified. At the start of every call:
//!
//! * a marker means that an earlier call did not finish. Every file of the database is removed, and
//!   the copy is made again from the source, which was not touched. If the source is gone by then,
//!   the call fails and the marker stays, so that a database that is not there is never taken for
//!   an empty one;
//! * no marker and a database means that the application has its database, and whatever the source
//!   holds, it is not read, and nothing is written;
//! * no marker and no database is a copy to be made, if there is a source.
//!
//! The marker is removed only after every file of the database was made durable, one by one: a
//! flush of a file in a pool covers that file and no other, so the removal of the marker, which is
//! one more flush of one more file, would not order the data before it.
//!
//! # Other pages
//!
//! Another tab or worker can call the function at the same time. The lock of the pool is held while
//! its files are mounted, and not between two mounts, so what an earlier look at the pool said is
//! never relied on once the files are mounted: the marker and the files are looked at again after
//! every mount, and the decision is made on that. The copy is checked in a second mount (what a
//! restart would find), with the lock let go in between, so there the marker is the guard: if it is
//! gone, another context finished the copy, or the database was removed, in that gap, and this call
//! checks nothing, removes nothing and writes nothing (it is `Existing`, or it fails if there is
//! no database). A database is removed after a copy only while the marker is still there.
//!
//! # Opening the database first
//!
//! The database is to be opened by nothing but this module until the copy is over, and the engine
//! sees to it: `TreeBuilder` refuses to open a database in a pool that has the marker, with an
//! `Error::MigrationFailed` that names the unfinished copy, and the copy opens the database itself,
//! past that refusal. An application that opens the database before it makes its first call has a
//! database, and the call finds it and copies nothing. An application that opens it while a copy is
//! unfinished is refused, and what the files hold is not touched. So the call comes first, at every
//! start, and an application that gets an error from it does not open the database. A copy that
//! fails also removes the files that it made, as far as it can, and keeps the marker.

use std::fmt::Display;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use surrealkv_compat_indxdb::{Chunk, ChunkFill, ChunkLimits};

use super::Digest;
use crate::error::{Error, Result};
use crate::vfs::{CreateOpts, EntryKind, Fs};
use crate::{Durability, FileSystem, InternalKeyKind, LSMIterator, Mode, Options, Tree};

/// The name of the file, at the root of the pool, whose presence says that a copy from IndexedDB
/// was begun and not finished. The engine knows it, to refuse to open what is there.
pub(crate) const MARKER: &str = super::INDEXEDDB_MARKER;

/// What the marker holds, for whoever looks at the files of a pool.
const MARKER_CONTENT: &[u8] =
	b"SurrealKV: a copy of an IndexedDB database into this one has not finished.\n";

/// The most bytes of keys and values in one transaction that writes to the database.
const WRITE_BATCH_BYTES: usize = 1 << 20;

/// The most entries in one transaction that writes to the database.
const WRITE_BATCH_ENTRIES: usize = 2_000;

/// How many times a commit that the engine could not take yet is tried before the copy fails.
const COMMIT_ATTEMPTS: usize = 200;

/// How long a commit that the engine could not take yet waits before it is tried again.
const COMMIT_PAUSE: Duration = Duration::from_millis(10);

/// What a call did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum IndexedDbMigration {
	/// There is no database to open and no IndexedDB database to copy: nothing was made, and the
	/// application starts with an empty database.
	NoSource,
	/// The database is there already, and this call did not change it. Nothing was read from
	/// IndexedDB, but for one case: another page finished a copy while this call was checking its
	/// own, and this call, which had read the rows, left the database of that page as it was.
	Existing,
	/// The rows of the IndexedDB database were copied into a database that did not exist, and the
	/// copy was checked: the database holds the same rows, and no others.
	Imported {
		/// How many rows were copied. An IndexedDB database that has the object store and no row
		/// in it is a copy of none, and the database that it makes is empty.
		entries: u64,
		/// The length of the keys and of the values of those rows, added up.
		bytes: u64,
	},
}

/// How a copy is cut into pieces. The values that [`ImportTuning::default`] has are what an
/// application gets; the fields are public for the tests, which cut the same work into small
/// pieces, and stop it half way.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct ImportTuning {
	/// How much one read of the source holds.
	pub read: ChunkLimits,
	/// How much one transaction on the database holds.
	pub write: ChunkLimits,
	/// How long a commit that the engine could not take yet waits before it is tried again.
	pub commit_pause: Duration,
	/// Ends the call as a worker that dies does, with the files let go and nothing flushed, after
	/// this many chunks were read and written. A test of the next call.
	pub abandon_after_chunks: Option<usize>,
}

impl Default for ImportTuning {
	fn default() -> Self {
		Self {
			read: ChunkLimits::default(),
			write: ChunkLimits::new(WRITE_BATCH_BYTES, WRITE_BATCH_ENTRIES),
			commit_pause: COMMIT_PAUSE,
			abandon_after_chunks: None,
		}
	}
}

/// What a call did to the source and to the database, for the tests.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ImportReport {
	/// Chunks that were read from the source.
	pub chunks: usize,
	/// Transactions that were committed to the database.
	pub transactions: usize,
}

/// The IndexedDB database that is moved, as far as the move needs to see it.
pub(crate) trait Source {
	/// Where the next read continues from: whatever the source gave with the last row. The move
	/// never looks inside it.
	type Cursor;

	/// Whether the database and its store are there. A source that is not there is not made by
	/// asking.
	async fn exists(&self) -> Result<bool>;

	/// The chunk of rows after `after`, or from the first row if there is none. `None` if the
	/// source is gone.
	async fn read_chunk(
		&self,
		after: Option<&Self::Cursor>,
		limits: &ChunkLimits,
	) -> Result<Option<Chunk<Self::Cursor>>>;
}

/// The files of the database that is made, as far as the move needs to see them.
pub(crate) trait Mounter {
	/// Whether the files are there at all, without making them.
	async fn exists(&self) -> Result<bool>;

	/// Mounts the files, and makes them if they are not there. A mount that finds them locked by
	/// another page fails with the error for which `Error::is_database_locked` is true.
	async fn mount(&self) -> Result<FileSystem>;
}

fn failed(what: &str, error: impl Display) -> Error {
	Error::MigrationFailed(format!("{what}: {error}"))
}

fn marker_path() -> &'static Path {
	Path::new(MARKER)
}

/// A mounted file system that is let go, without a flush, if the call that holds it ends in any
/// other way than [`Mount::close`]: an error, or a future that is dropped. What was not flushed is
/// not needed, since the marker says that the database is to be made again.
struct Mount {
	fs: Option<FileSystem>,
}

impl Mount {
	fn fs(&self) -> &FileSystem {
		self.fs.as_ref().expect("a mount is open until it is closed")
	}

	fn dyn_fs(&self) -> &dyn Fs {
		&*self.fs().0
	}

	/// Flushes the files and lets them go. Fails if the flush did.
	async fn close(mut self) -> Result<()> {
		let fs = self.fs.take().expect("a mount is open until it is closed");
		fs.close().await
	}
}

impl Drop for Mount {
	fn drop(&mut self) {
		if let Some(fs) = self.fs.take() {
			fs.abandon();
		}
	}
}

/// A tree that is let go the way a crash lets it go if the call that holds it ends in any other way
/// than [`OpenTree::close`].
struct OpenTree {
	tree: Option<Tree>,
}

impl OpenTree {
	fn tree(&self) -> &Tree {
		self.tree.as_ref().expect("a tree is open until it is closed")
	}

	async fn close(mut self) -> Result<()> {
		let tree = self.tree.take().expect("a tree is open until it is closed");
		tree.close().await
	}

	/// Closes the tree after a failure, as far as that goes: what is not needed is not flushed to
	/// matter, and it is not an error if the close fails too. What it is for is that no task of the
	/// tree is left running over the files that are removed next.
	async fn end(self) {
		let _ = self.close().await;
	}
}

impl Drop for OpenTree {
	fn drop(&mut self) {
		if let Some(tree) = self.tree.take() {
			tree.abandon();
		}
	}
}

async fn mount<M: Mounter>(mounter: &M) -> Result<Mount> {
	match mounter.mount().await {
		Ok(fs) => Ok(Mount {
			fs: Some(fs),
		}),
		// The one error that the caller can act on, with its type.
		Err(e) if e.is_database_locked() => Err(e),
		Err(e) => Err(failed("mounting the files of the database", e)),
	}
}

/// Opens the database in the files of `files`. It does not go through `TreeBuilder`, which refuses
/// a pool that has the marker: this is the one open that is meant to find it there.
async fn open_tree(files: &Mount, options: &Options) -> Result<OpenTree> {
	let options = Options {
		fs: std::sync::Arc::clone(&files.fs().0),
		..options.clone()
	};
	let tree = Tree::open_async(std::sync::Arc::new(options))
		.await
		.map_err(|e| failed("opening the database", e))?;
	Ok(OpenTree {
		tree: Some(tree),
	})
}

// ---------------------------------------------------------------------------------------------
// The files
// ---------------------------------------------------------------------------------------------

/// The files and the directories of the pool, but for the marker, below both of its roots (a pool
/// keeps a path that begins with a separator apart from one that does not).
fn collect(fs: &dyn Fs, files: &mut Vec<PathBuf>, dirs: &mut Vec<PathBuf>) -> io::Result<()> {
	fn walk(
		fs: &dyn Fs,
		dir: &Path,
		files: &mut Vec<PathBuf>,
		dirs: &mut Vec<PathBuf>,
	) -> io::Result<()> {
		let entries = match fs.read_dir(dir) {
			Ok(entries) => entries,
			Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
			Err(e) => return Err(e),
		};
		for entry in entries {
			let path = dir.join(&entry.name);
			if entry.kind == EntryKind::Dir {
				dirs.push(path.clone());
				walk(fs, &path, files, dirs)?;
			} else if path != marker_path() {
				files.push(path);
			}
		}
		Ok(())
	}
	walk(fs, Path::new(""), files, dirs)?;
	walk(fs, Path::new("/"), files, dirs)
}

/// What the pool holds, as far as the rules of a call need to tell.
struct Found {
	marker: bool,
	database: bool,
}

fn inspect(fs: &dyn Fs) -> io::Result<Found> {
	let marker = fs.try_exists(marker_path())?;
	let (mut files, mut dirs) = (Vec::new(), Vec::new());
	collect(fs, &mut files, &mut dirs)?;
	Ok(Found {
		marker,
		database: !files.is_empty(),
	})
}

/// Waits for nothing: cleans the slots of the files that were removed, which makes the removals
/// durable, and fails if any is left that could not be.
fn settle_removals(fs: &dyn Fs) -> io::Result<()> {
	let Some(pool) = fs.pool() else {
		return Ok(());
	};
	pool.maintain(usize::MAX);
	let stats = pool.stats();
	if stats.dirty != 0 || stats.pinned != 0 {
		return Err(io::Error::other(format!(
			"{} removed files could not be cleaned and {} are still open",
			stats.dirty, stats.pinned
		)));
	}
	Ok(())
}

/// Removes every file of the database, and the directories of it, and makes the removals durable.
/// The marker is not touched.
fn wipe(fs: &dyn Fs) -> io::Result<()> {
	let (mut files, mut dirs) = (Vec::new(), Vec::new());
	collect(fs, &mut files, &mut dirs)?;
	for path in &files {
		match fs.remove_file(path) {
			Ok(()) => {}
			Err(e) if e.kind() == io::ErrorKind::NotFound => {}
			Err(e) => return Err(e),
		}
	}
	// The deepest first. A directory of a pool is a name that its files give it, so this only
	// forgets the names.
	dirs.sort();
	for dir in dirs.iter().rev() {
		match fs.remove_dir_all(dir) {
			Ok(()) => {}
			Err(e) if e.kind() == io::ErrorKind::NotFound => {}
			Err(e) => return Err(e),
		}
	}
	settle_removals(fs)?;
	let (mut left, mut dirs_left) = (Vec::new(), Vec::new());
	collect(fs, &mut left, &mut dirs_left)?;
	if !left.is_empty() {
		return Err(io::Error::other(format!(
			"{} files of the database are still there",
			left.len()
		)));
	}
	Ok(())
}

/// Makes the marker, and makes it and its name durable. A marker that is there is made durable
/// again.
fn write_marker(fs: &dyn Fs) -> io::Result<()> {
	match fs.create_new(marker_path(), CreateOpts::default()) {
		Ok(file) => {
			file.append(MARKER_CONTENT)?;
			file.sync()?;
		}
		Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
		Err(e) => return Err(e),
	}
	fs.sync_file(marker_path())?;
	fs.sync_dir(Path::new(""))
}

/// Makes every file of the database durable, each one: the flush of a file in a pool is the flush
/// of that file, so the data cannot be ordered before the removal of the marker by one flush.
fn make_durable(fs: &dyn Fs) -> io::Result<()> {
	let (mut files, mut dirs) = (Vec::new(), Vec::new());
	collect(fs, &mut files, &mut dirs)?;
	for path in &files {
		fs.sync_file(path)?;
	}
	for dir in dirs.iter().map(PathBuf::as_path).chain([Path::new(""), Path::new("/")]) {
		fs.sync_dir(dir)?;
	}
	settle_removals(fs)
}

/// Removes the marker, and makes the removal durable: the commit of the copy.
fn remove_marker(fs: &dyn Fs) -> io::Result<()> {
	fs.remove_file(marker_path())?;
	settle_removals(fs)?;
	if fs.try_exists(marker_path())? {
		return Err(io::Error::other("the marker is still there after it was removed"));
	}
	Ok(())
}

// ---------------------------------------------------------------------------------------------
// What the copy says about itself
// ---------------------------------------------------------------------------------------------

/// What a pass over a set of rows saw: how many, how long, and three things that tell them apart.
///
/// * `sum` is a sum of the hashes of the rows one by one, which does not depend on the order. The
///   order of a source (a string key sorts by UTF-16 units, and keys of different types by type) is
///   not the order of the keys as bytes, so for a source that does not give them in byte order this
///   is all that can be compared, and a verification must not fail a copy for the order.
/// * `ascending` is whether every key was greater than the one before as bytes, which is the order
///   of a database whatever the source did: the rows of the database must be so, and so must the
///   rows that were read, if they are to be compared in sequence.
/// * `chain` is a hash of the rows in the sequence that they came in. It is compared when the rows
///   that were read are ascending too, and then the two sequences are the same sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Rows {
	pub(crate) entries: u64,
	pub(crate) bytes: u64,
	pub(crate) sum: u64,
	pub(crate) chain: u64,
	pub(crate) ascending: bool,
}

/// A pass over rows, which gives [`Rows`] when it is over.
pub(crate) struct Tally {
	entries: u64,
	bytes: u64,
	sum: u64,
	chain: Digest,
	/// The last key, while the keys are ascending.
	last: Option<Vec<u8>>,
	ascending: bool,
}

impl Tally {
	pub(crate) fn new() -> Self {
		Self {
			entries: 0,
			bytes: 0,
			sum: 0,
			chain: Digest::new(),
			last: None,
			ascending: true,
		}
	}

	pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) {
		let mut one = Digest::new();
		one.add(key, 0, InternalKeyKind::Set, None, false, value);
		let (_, hash) = one.finish();
		self.sum = self.sum.wrapping_add(hash);
		self.chain.add(key, 0, InternalKeyKind::Set, None, false, value);
		self.entries += 1;
		self.bytes += (key.len() + value.len()) as u64;
		if self.ascending {
			match &mut self.last {
				Some(last) if key <= last.as_slice() => {
					self.ascending = false;
					self.last = None;
				}
				Some(last) => {
					last.clear();
					last.extend_from_slice(key);
				}
				None => self.last = Some(key.to_vec()),
			}
		}
	}

	pub(crate) fn finish(&self) -> Rows {
		Rows {
			entries: self.entries,
			bytes: self.bytes,
			sum: self.sum,
			chain: self.chain.finish().1,
			ascending: self.ascending,
		}
	}
}

/// `Ok` if the database holds what was read, and the error that names what differs if not.
pub(crate) fn compare(read: &Rows, found: &Rows) -> Result<()> {
	let hint =
		"if the database holds fewer, two rows of the source have the same key once it is bytes";
	if read.entries != found.entries {
		return Err(Error::MigrationFailed(format!(
			"the copy does not match the source: {} rows were read and the database holds {} ({hint})",
			read.entries, found.entries
		)));
	}
	if read.bytes != found.bytes {
		return Err(Error::MigrationFailed(format!(
			"the copy does not match the source: the {} rows read are {} bytes of keys and values, and the database holds {}",
			read.entries, read.bytes, found.bytes
		)));
	}
	if read.sum != found.sum {
		return Err(Error::MigrationFailed(format!(
			"the copy does not match the source: the database holds {} rows like the source, and their contents differ (digest {:016x} against {:016x})",
			read.entries, found.sum, read.sum
		)));
	}
	// The rows of a database are in the order of their keys. A database that gives them in another
	// order is not one, whatever it holds.
	if !found.ascending {
		return Err(Error::MigrationFailed(
			"the copy does not match the source: the database gives its rows in another order than that of their keys"
				.to_string(),
		));
	}
	// A source that gave its keys in the order of the bytes is compared in sequence. One that did
	// not (a store of strings or of keys of several types) has only had its rows compared as a set.
	if read.ascending && read.chain != found.chain {
		return Err(Error::MigrationFailed(format!(
			"the copy does not match the source: the {} rows of the database, in the order of their keys, are not the rows that were read (digest of the sequence {:016x} against {:016x})",
			read.entries, found.chain, read.chain
		)));
	}
	Ok(())
}

// ---------------------------------------------------------------------------------------------
// The copy
// ---------------------------------------------------------------------------------------------

/// Whether a failed commit is one that the engine can take again in a moment.
fn is_passing(error: &Error) -> bool {
	matches!(error, Error::PipelineStall | Error::WriteStall { .. } | Error::FilePoolExhausted)
}

/// What a transaction that cannot take a row says: an empty key is a fault of the source, which the
/// database cannot hold, and anything else is the engine's.
fn row_error(error: Error) -> Error {
	match error {
		Error::EmptyKey => Error::MigrationFailed(
			"the source has a row whose key is empty, which a SurrealKV database cannot hold"
				.to_string(),
		),
		other => failed("writing a row", other),
	}
}

/// Commits `batch` in one transaction, again as often as the engine says that it cannot take it
/// yet. A transaction that failed to commit is closed, so each try is a new one.
async fn commit_batch(
	tree: &Tree,
	batch: &[(Vec<u8>, Vec<u8>)],
	tuning: &ImportTuning,
) -> Result<()> {
	let mut attempt = 1;
	loop {
		let mut txn = tree.begin().map_err(|e| failed("beginning a transaction", e))?;
		txn.set_durability(Durability::Eventual);
		for (key, value) in batch {
			txn.set(&key[..], &value[..]).map_err(row_error)?;
		}
		match txn.commit().await {
			Ok(()) => return Ok(()),
			Err(e) if is_passing(&e) && attempt < COMMIT_ATTEMPTS => {
				attempt += 1;
				crate::rt::sleep(tuning.commit_pause).await;
			}
			Err(e) => return Err(failed("writing rows to the database", e)),
		}
	}
}

/// Writes `entries` in transactions of the size that `tuning` says, and counts them.
async fn write_chunk(
	tree: &Tree,
	entries: Vec<(Vec<u8>, Vec<u8>)>,
	tuning: &ImportTuning,
	report: &mut ImportReport,
) -> Result<()> {
	let mut rest = entries.into_iter().peekable();
	while rest.peek().is_some() {
		let mut fill = ChunkFill::new(tuning.write);
		let mut batch = Vec::new();
		while let Some((key, value)) = rest.peek() {
			let weight = key.len().saturating_add(value.len());
			if !fill.admits(weight) {
				break;
			}
			fill.take(weight);
			if let Some(row) = rest.next() {
				batch.push(row);
			}
			if fill.is_full() {
				break;
			}
		}
		commit_batch(tree, &batch, tuning).await?;
		report.transactions += 1;
	}
	Ok(())
}

/// How the reading and writing of the rows ended, when it did not fail.
enum Copied {
	/// Every row of the source was written, and the tree was closed and so flushed. What was read.
	Whole(Rows),
	/// The tuning said to stop, as a worker that dies stops: nothing was flushed, and nothing is
	/// removed. After this many chunks.
	CutShort(usize),
}

/// Reads the source to its end, chunk by chunk, and writes each chunk before the next is read.
/// Returns what was read, or `None` if the tuning says to stop before the end.
async fn write_all<S: Source>(
	source: &S,
	tree: &Tree,
	tuning: &ImportTuning,
	report: &mut ImportReport,
) -> Result<Option<Rows>> {
	let mut read = Tally::new();
	let mut after: Option<S::Cursor> = None;
	loop {
		let chunk = source.read_chunk(after.as_ref(), &tuning.read).await?.ok_or_else(|| {
			Error::MigrationFailed(
				"the IndexedDB database, or its store kv, went away while it was read".to_string(),
			)
		})?;
		report.chunks += 1;
		if chunk.entries.is_empty() && !chunk.done {
			return Err(Error::MigrationFailed(
				"the IndexedDB database gave an empty chunk before its end".to_string(),
			));
		}
		for (key, value) in &chunk.entries {
			read.add(key, value);
		}
		let (done, last) = (chunk.done, chunk.last);
		write_chunk(tree, chunk.entries, tuning, report).await?;
		if tuning.abandon_after_chunks.is_some_and(|n| report.chunks >= n) {
			return Ok(None);
		}
		if done {
			return Ok(Some(read.finish()));
		}
		if last.is_none() {
			return Err(Error::MigrationFailed(
				"the IndexedDB database did not say where its chunk ended".to_string(),
			));
		}
		after = last;
	}
}

/// Opens the database in `files`, writes the rows of the source to it, and closes it, which
/// flushes it. A tree that a failure leaves is closed too, so that nothing of it runs on while the
/// files are removed.
async fn copy<S: Source>(
	source: &S,
	files: &Mount,
	options: &Options,
	tuning: &ImportTuning,
	report: &mut ImportReport,
) -> Result<Copied> {
	let tree = open_tree(files, options).await?;
	match write_all(source, tree.tree(), tuning, report).await {
		Ok(Some(read)) => {
			tree.close().await.map_err(|e| failed("closing the database", e))?;
			Ok(Copied::Whole(read))
		}
		// Dropped, and so let go as a crash lets go.
		Ok(None) => Ok(Copied::CutShort(report.chunks)),
		Err(e) => {
			tree.end().await;
			Err(e)
		}
	}
}

/// Reads every row of the database that is in `mount`, as a verification does: the rows that are
/// there, counted and digested, with the values that the value log holds put back.
async fn scan(files: &Mount, options: &Options) -> Result<Rows> {
	let tree = open_tree(files, options).await?;
	match read_rows(tree.tree()) {
		Ok(found) => {
			tree.close().await.map_err(|e| failed("closing the database", e))?;
			Ok(found)
		}
		Err(e) => {
			tree.end().await;
			Err(e)
		}
	}
}

fn read_rows(tree: &Tree) -> Result<Rows> {
	let mut found = Tally::new();
	let txn =
		tree.begin_with_mode(Mode::ReadOnly).map_err(|e| failed("reading the database", e))?;
	let mut rows = txn.iter().map_err(|e| failed("reading the database", e))?;
	let mut valid = rows.seek_first().map_err(|e| failed("reading the database", e))?;
	while valid {
		let value = rows.value().map_err(|e| failed("reading the database", e))?;
		found.add(rows.key().user_key(), &value);
		valid = rows.next().map_err(|e| failed("reading the database", e))?;
	}
	Ok(found.finish())
}

/// What a copy needs of the database that it makes.
fn check_path(options: &Options) -> Result<()> {
	let names: Vec<&std::ffi::OsStr> = options
		.path
		.components()
		.filter_map(|c| match c {
			Component::Normal(name) => Some(name),
			_ => None,
		})
		.collect();
	match names.first() {
		None => Err(Error::MigrationFailed(
			"the path of the database must name a directory of the pool, and not its root: the marker of a copy is kept at the root"
				.to_string(),
		)),
		Some(first) if *first == MARKER => Err(Error::MigrationFailed(format!(
			"the path of the database cannot begin with {MARKER:?}, which is the name of the marker of a copy"
		))),
		Some(_) => Ok(()),
	}
}

/// Removes what a copy that failed left, as far as that can be done, and gives back the error that
/// the copy failed with, with what could not be removed added to it. The marker stays, which is
/// what makes whatever is left not a database, for the engine and for the next call.
async fn discard(files: Mount, error: Error) -> Error {
	let wiped = wipe(files.dyn_fs());
	let _ = files.close().await;
	match wiped {
		Ok(()) => error,
		Err(e) => {
			Error::MigrationFailed(format!("{error}; and the database could not be removed: {e}"))
		}
	}
}

/// Makes the database from the source, in a pool that has no database and, but for a marker that
/// an earlier call left, nothing. Ends with the marker gone and the files let go.
async fn import<S: Source, M: Mounter>(
	source: &S,
	mounter: &M,
	first: Mount,
	options: &Options,
	tuning: &ImportTuning,
	report: &mut ImportReport,
) -> Result<IndexedDbMigration> {
	// Before any other file of the database: the marker, durably.
	write_marker(first.dyn_fs()).map_err(|e| failed("making the marker of the copy", e))?;

	let read = match copy(source, &first, options, tuning, report).await {
		Ok(Copied::Whole(read)) => read,
		Ok(Copied::CutShort(chunks)) => {
			return Err(Error::MigrationFailed(format!(
				"stopped after {chunks} chunks, as a worker that dies would"
			)));
		}
		// The files of the copy are removed, and the marker is not.
		Err(e) => return Err(discard(first, e).await),
	};
	// The flush that makes the copy durable as the engine knows how to, and the release of the
	// files. A mount that comes after it is what a restart would find.
	first.close().await.map_err(|e| failed("closing the files of the database", e))?;

	// The lock was let go, and another page may have had it since: what is in the pool is looked at
	// again, and a copy that is not there to be checked is not checked.
	let second = mount(mounter).await?;
	let now = inspect(second.dyn_fs()).map_err(|e| failed("looking at the database", e))?;
	if !now.marker {
		// Another page found the marker, made the copy again, checked it, and committed it, or the
		// files were removed. What is there is not this call's to check, to remove or to commit.
		let _ = second.close().await;
		return if now.database {
			Ok(IndexedDbMigration::Existing)
		} else {
			Err(Error::MigrationFailed(
				"the files of the copy were removed by another page before the copy could be checked. \
				 No database was made. Calling again makes the copy again"
					.to_string(),
			))
		};
	}
	let found = match scan(&second, options).await {
		Ok(found) => found,
		Err(e) => return Err(discard(second, e).await),
	};
	if let Err(mismatch) = compare(&read, &found) {
		// The database is not what the source was: remove it, and say so. The marker stays, so
		// that nothing takes what is left for a database if this is interrupted.
		return Err(discard(second, mismatch).await);
	}

	// The commit: every file of the database is durable, and then the marker is gone. A failure
	// from here on does not remove the database, which is whole: with the marker still there
	// the engine does not open it and the next call makes it again, and with the marker gone it
	// is the database.
	make_durable(second.dyn_fs()).map_err(|e| failed("making the database durable", e))?;
	remove_marker(second.dyn_fs()).map_err(|e| failed("removing the marker of the copy", e))?;
	// The copy is committed. A flush that fails now changes nothing about what is in the pool.
	if let Err(e) = second.close().await {
		tracing::warn!("closing the files after a copy from IndexedDB failed: {e}");
	}
	Ok(IndexedDbMigration::Imported {
		entries: read.entries,
		bytes: read.bytes,
	})
}

/// The whole of a call: see the module.
pub(crate) async fn run<S: Source, M: Mounter>(
	source: &S,
	mounter: &M,
	options: &Options,
	tuning: &ImportTuning,
	report: &mut ImportReport,
) -> Result<IndexedDbMigration> {
	check_path(options)?;
	let present = mounter.exists().await.map_err(|e| failed("looking for the database", e))?;

	// A pool that is not there is not made to find out that it is empty. The answer only spares
	// that: once the files are mounted, what is in them is looked at, whatever this said, because
	// another page may have made them, and used them, since.
	if !present && !source.exists().await? {
		return Ok(IndexedDbMigration::NoSource);
	}

	let first = mount(mounter).await?;
	let found = inspect(first.dyn_fs()).map_err(|e| failed("looking at the database", e))?;
	if found.marker {
		// An earlier call did not finish, and what it made is not a database.
		wipe(first.dyn_fs()).map_err(|e| failed("removing the files of an unfinished copy", e))?;
		if !source.exists().await? {
			let _ = first.close().await;
			return Err(Error::MigrationFailed(
				"an earlier copy of the IndexedDB database was not finished, its files were removed, \
				 and the IndexedDB database is not there to be copied again. No database was made. \
				 Remove the files of the database (FileSystem::destroy_opfs) to start without one"
					.to_string(),
			));
		}
		return import(source, mounter, first, options, tuning, report).await;
	}
	if found.database {
		let _ = first.close().await;
		return Ok(IndexedDbMigration::Existing);
	}
	if !source.exists().await? {
		let _ = first.close().await;
		return Ok(IndexedDbMigration::NoSource);
	}
	import(source, mounter, first, options, tuning, report).await
}

// ---------------------------------------------------------------------------------------------
// The browser
// ---------------------------------------------------------------------------------------------

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use self::browser::{migrate_from_indexeddb, migrate_from_indexeddb_with_tuning};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod browser {
	use surrealkv_compat_indxdb::wasm::IdbSource;
	use wasm_bindgen::JsValue;

	use super::*;
	use crate::vfs::opfs_raw::OpfsRaw;

	/// The rows of the IndexedDB database `name`.
	struct IndexedDb(IdbSource);

	impl Source for IndexedDb {
		type Cursor = JsValue;

		async fn exists(&self) -> Result<bool> {
			self.0.exists().await.map_err(|e| failed("looking for the IndexedDB database", e))
		}

		async fn read_chunk(
			&self,
			after: Option<&JsValue>,
			limits: &ChunkLimits,
		) -> Result<Option<Chunk<JsValue>>> {
			self.0
				.read_chunk(after, limits)
				.await
				.map_err(|e| failed("reading the IndexedDB database", e))
		}
	}

	/// The database `name` of the origin private file system.
	struct Opfs {
		name: String,
	}

	impl Mounter for Opfs {
		async fn exists(&self) -> Result<bool> {
			Ok(OpfsRaw::open_existing(&self.name).await?.is_some())
		}

		async fn mount(&self) -> Result<FileSystem> {
			FileSystem::opfs(&self.name).await
		}
	}

	/// Moves the rows of the legacy IndexedDB database `indexeddb` (the object store `kv` that the
	/// `indxdb://` backend of SurrealDB wrote) into the database `opfs` of the origin private file
	/// system, unless that exists, and says what it did.
	///
	/// Call it when the application starts, in the dedicated worker that is to use the database,
	/// **before** the database is opened, every time: with the database in place it mounts its
	/// files and does nothing else. The database is made with `options` (its `path`, and what else
	/// the application opens it with); the application opens it with the same `path`
	/// afterwards, over `FileSystem::opfs(opfs)`. The `fs` of `options` is not used.
	///
	/// If the call fails, do not open the database. A copy that was cut short leaves a marker, and
	/// `TreeBuilder` refuses to open a database in a pool that has it, with an
	/// `Error::MigrationFailed` that names the unfinished copy: what is there is part of a
	/// copy, which the next call removes and makes again.
	///
	/// * [`IndexedDbMigration::NoSource`]: there is no database called `opfs`, and no IndexedDB
	///   database called `indexeddb` with an object store `kv`. Nothing was made, and the
	///   application starts with a database of its own.
	/// * [`IndexedDbMigration::Existing`]: the database `opfs` is there, and was not changed.
	///   IndexedDB was not looked at. This is the answer of every call after the first, and of one
	///   that comes after the application opened the database itself, which it should not have (the
	///   data of IndexedDB is not moved in that case).
	/// * [`IndexedDbMigration::Imported`]: the rows were copied, and the copy was checked against
	///   what was read: the database holds the same rows, and no others.
	///
	/// The IndexedDB database is only read: it is not created, changed or deleted, by a success
	/// either, and it is left for the application to remove when it wants to. Do not write to it
	/// while it is copied.
	///
	/// A call that was interrupted (the page was closed, the worker was terminated, the power went)
	/// leaves a marker file in the database `opfs`, and the next call removes whatever is there and
	/// copies again from IndexedDB. If IndexedDB has no such database by then, the call fails and
	/// no database is made; `FileSystem::destroy_opfs` removes what is left, to start without one.
	///
	/// # Errors
	///
	/// [`Error::MigrationFailed`], with a message that says what failed, if the copy could not be
	/// made, or was made and does not match the source (the database is removed in that case), and
	/// an error for which [`Error::is_database_locked`] is true if another tab or worker has the
	/// database `opfs` open. Calling again is the way to try again.
	pub async fn migrate_from_indexeddb(
		indexeddb: &str,
		opfs: &str,
		options: Options,
	) -> Result<IndexedDbMigration> {
		migrate_from_indexeddb_with_tuning(indexeddb, opfs, options, ImportTuning::default())
			.await
			.0
	}

	/// [`migrate_from_indexeddb`] with the sizes of the pieces that it works in, and what it did
	/// to the source and to the database, for the tests.
	#[doc(hidden)]
	pub async fn migrate_from_indexeddb_with_tuning(
		indexeddb: &str,
		opfs: &str,
		options: Options,
		tuning: ImportTuning,
	) -> (Result<IndexedDbMigration>, ImportReport) {
		let mut report = ImportReport::default();
		let source = IndexedDb(IdbSource::new(indexeddb));
		let files = Opfs {
			name: opfs.to_string(),
		};
		let outcome = run(&source, &files, &options, &tuning, &mut report).await;
		(outcome, report)
	}
}
