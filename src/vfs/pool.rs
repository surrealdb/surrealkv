//! The pool: a file system made of a fixed set of pre-opened slot files.
//!
//! A browser's origin private file system can only be opened, created, renamed and removed
//! asynchronously, and a file that is open cannot be renamed or removed at all, while the engine
//! calls its file system synchronously, under locks. The pool resolves it by keeping the names in
//! memory. Every slot is one physical file, opened once and held; a logical file is a slot with a
//! name in its header, and `create`, `rename`, `remove`, `exists` and `read_dir` are changes of an
//! in-memory registry that are made durable by writing the header of a slot. Only two things are
//! asynchronous: mounting (opening every slot) and refilling (making blank slots).
//!
//! The protocols, the crash analysis of each, and the layout are in the plan of the browser port
//! (section 6). In short:
//!
//! * a slot is `[page A][page B][payload]`, see [`super::header`]; the logical length of a file is
//!   the physical size minus the two pages, and the bytes of a file are exactly what a native file
//!   would hold;
//! * a header update writes the page that does not count, with a sequence number that only grows
//!   for the whole pool, so a torn write leaves the old header readable;
//! * a slot is claimed from a list of clean slots (free headers, no payload). A slot that is
//!   unlinked is cleaned before it is reused: truncated to the headers, both pages made free,
//!   flushed. Nothing is cleaned in a `Drop`: a dead slot is queued, and cleaned by `maintain` or,
//!   if a claim finds nothing clean, by the claim;
//! * `rename`, and the replacement of a manifest, commit in one flush of one slot, so there is no
//!   ordering between two files and no directory to sync;
//! * a mount reads every header, and a path that two slots claim goes to the one with the higher
//!   sequence number and the other is cleaned. A slot with no valid header and a payload is an
//!   orphan: it is kept, reported, and never used.
//!
//! The backend of a pool is a [`Raw`]: a set of files with positional reads and writes that are not
//! atomic and are lost in a crash unless they were flushed. [`super::mem::MemRaw`] is one in
//! memory, which is what the tests of the pool and the in-memory file system run over, and the
//! origin private file system of a browser is the other.
//!
//! # Locking
//!
//! The registry (`ns`) is taken before a slot's state (`st`), and before the free lists (`free`);
//! the state of a slot before the free lists; nothing is held while awaiting. `Drop` of a file
//! takes the state of its slot and then the free lists, and never the registry.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::path::{Component, Path};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use std::{fmt, io};

use parking_lot::Mutex;
use tokio::sync::Notify;

use super::header::{
	page_offset,
	Header,
	SlotHeaders,
	SlotState as HeaderState,
	HEADER_BYTES,
	HEADER_LEN,
	MAX_PATH_LEN,
};
use super::mem::CrashMode;
use super::{
	into_io_error,
	pool_exhausted,
	pool_full,
	CreateOpts,
	DirEntry,
	EntryKind,
	Fs,
	FsFile,
	FsKind,
	SpareClass,
	SpareWait,
};
use crate::error::{Error, Result};
use crate::lockfile::{LockFile, LockRegistry};
use crate::rt;
use crate::vfs::File;

/// The name of the lock file of a pool, in the directory of its slots.
pub(crate) const LOCK_NAME: &str = "LOCK";

/// The most slots that a refill makes in one go.
const REFILL_BATCH: usize = 16;

/// The most slots that a mount opens at once.
const OPEN_BATCH: usize = 256;

/// A future of a backend. `Send` where there are threads, and not in a browser, where the futures
/// of a file system are those of JavaScript. A backend must make these safe to drop: a creation or
/// an open that is dropped after it was asked for must close the file when it arrives.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) type RawFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) type RawFut<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// One open slot file of a backend: positional, and not atomic. What was written is lost in a crash
/// until `flush` returns.
///
/// A handle that is dropped without `close` may stay locked, as the handles of a browser do until
/// they are collected, so a pool closes every handle it holds.
pub(crate) trait RawFile: Send + Sync + 'static {
	/// Reads at `offset`; fewer bytes than asked for only at the end of the file.
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
	/// Writes at `offset`, and returns how many bytes were written, which may be fewer than given.
	/// A file that is written past its end is zero-filled up to the write.
	fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<usize>;
	/// Shrinks or grows (with zeros) the file.
	fn set_len(&self, len: u64) -> io::Result<()>;
	fn size(&self) -> io::Result<u64>;
	/// A barrier: every write and truncation before it is ordered before every one after it, and
	/// made durable as far as the platform says. Not a promise about a power cut.
	fn flush(&self) -> io::Result<()>;
	/// Releases the handle. Idempotent. Nothing is flushed.
	fn close(&self) -> io::Result<()>;
}

/// The backend of a pool: the directory of slot files.
///
/// The only operations that are asynchronous are the ones that a browser makes asynchronous:
/// listing, opening, creating, and taking the lock.
pub(crate) trait Raw: Send + Sync + Sized + 'static {
	type File: RawFile;

	/// The names of the slot files (the lock file is not one).
	fn list(&self) -> RawFut<'_, io::Result<Vec<String>>>;
	/// Opens an existing slot, exclusively.
	fn open(&self, name: &str) -> RawFut<'_, io::Result<Self::File>>;
	/// Creates an empty slot and opens it. `AlreadyExists` if it is there.
	fn create(&self, name: &str) -> RawFut<'_, io::Result<Self::File>>;
	/// Opens the lock file, creating it, exclusively: `PermissionDenied` if another handle holds
	/// it. It is held until the mount ends.
	fn open_lock(&self) -> RawFut<'_, io::Result<Self::File>>;
	/// Whether `create_sync` makes slots. A pool that is configured to make slots on the spot does
	/// so only over a backend that says it can.
	fn can_create_sync(&self) -> bool {
		false
	}
	/// A creation that does not wait, for a backend that has one: what lets a pool in memory make a
	/// slot when it is asked for one. `None` where creating is asynchronous.
	fn create_sync(&self, name: &str) -> Option<io::Result<Self::File>> {
		let _ = name;
		None
	}
	/// What a crash would leave of this backend, as a new backend, for a backend that can say.
	fn crash_image(&self, mode: CrashMode) -> Option<Self> {
		let _ = mode;
		None
	}
}

// ---------------------------------------------------------------------------------------------
// Configuration and reports
// ---------------------------------------------------------------------------------------------

/// How a pool is sized and refilled.
#[derive(Clone, Debug)]
pub(crate) struct PoolConfig {
	/// A claim that leaves fewer free slots than this asks for a refill.
	pub(crate) low: usize,
	/// A refill makes slots until this many are free.
	pub(crate) high: usize,
	/// The slots made when a pool is mounted over an empty directory.
	pub(crate) initial: usize,
	/// Refill in a task of its own, if there is a runtime to run it. Without it, only the code
	/// that waits for spares refills, so a missing wait fails in a test and not in a browser.
	pub(crate) background_refill: bool,
	/// The most slots that exist: a hard cap, with a typed error. Slots that are pinned by a
	/// snapshot of a table that was removed count.
	pub(crate) max_files: usize,
	/// Make a slot when a claim finds none, if the backend can do that without waiting.
	pub(crate) inline_provision: bool,
	/// The free slots that a claim of the `Opportunistic` class leaves.
	pub(crate) reserve: usize,
	/// How long a mount keeps trying to take the lock of a pool that another page holds. A holder
	/// that is in a long synchronous operation is killed by the browser after about two seconds.
	pub(crate) lock_deadline: Duration,
	/// The time between two tries.
	pub(crate) lock_poll: Duration,
}

impl Default for PoolConfig {
	fn default() -> Self {
		Self {
			low: 24,
			high: 48,
			initial: 48,
			background_refill: true,
			max_files: 2048,
			inline_provision: false,
			reserve: 8,
			lock_deadline: Duration::from_secs(5),
			lock_poll: Duration::from_millis(75),
		}
	}
}

impl PoolConfig {
	/// A pool in memory: slots are made when they are asked for, so it never waits and no task is
	/// needed, and a mount does not wait for a lock.
	pub(crate) fn memory() -> Self {
		Self {
			low: 0,
			high: 0,
			initial: 0,
			background_refill: false,
			inline_provision: true,
			lock_deadline: Duration::ZERO,
			..Self::default()
		}
	}

	/// A pool that is refilled only by the code that waits for spares, with a bounded number of
	/// slots: the configuration that makes a missing `ensure_spares` visible in a test.
	pub(crate) fn strict(initial: usize, max_files: usize) -> Self {
		Self {
			low: 0,
			high: initial,
			initial,
			background_refill: false,
			inline_provision: false,
			max_files,
			lock_deadline: Duration::ZERO,
			..Self::default()
		}
	}
}

/// What a mount found.
#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MountReport {
	/// The files that the pool has: slots with a name.
	pub files: usize,
	/// The slots that are free.
	pub spares: usize,
	/// The slots that were free or blank but were not clean, or that lost a path to a newer one,
	/// and were cleaned.
	pub losers_cleaned: usize,
	/// Slots that have no valid header and a payload. They are kept as they are and never used.
	pub orphans_kept: Vec<String>,
	/// The highest sequence number that any header had.
	pub max_seq: u64,
	/// Slots that were blank (too short for a header, or with no valid header and no payload) and
	/// were initialised.
	pub blank_initialised: usize,
	/// Slots that were free but had a payload, which was cut off.
	pub cut_back: usize,
}

/// What a pool holds now.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PoolStats {
	/// Slots that have a name.
	pub(crate) files: usize,
	/// Free slots that are clean.
	pub(crate) clean: usize,
	/// Dead slots that wait to be cleaned.
	pub(crate) dirty: usize,
	/// Slots that no name points at but a handle does: files removed while open, which a snapshot
	/// of a table keeps.
	pub(crate) pinned: usize,
	/// Slots that were kept at the mount for having no valid header.
	pub(crate) orphans: usize,
	/// Slots that a failed manifest write left in a state that is not known.
	pub(crate) poisoned: usize,
	/// Every slot that exists or is being made.
	pub(crate) total: usize,
	pub(crate) max_files: usize,
	pub(crate) exhaustions: u64,
}

// ---------------------------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------------------------

fn invalid(message: &'static str) -> io::Error {
	io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// The name of a path in the registry: components resolved by name, `/` between them, and a `/`
/// at the start of an absolute path. `a/./b`, `a//b` and `a/b/c/..` are what they say. Fails for a
/// path that is not UTF-8, or has a NUL in it.
pub(crate) fn key_of(path: &Path) -> io::Result<String> {
	let mut prefix = String::new();
	let mut rooted = false;
	let mut segments: Vec<&str> = Vec::new();
	for component in path.components() {
		match component {
			Component::Prefix(p) => {
				prefix = p
					.as_os_str()
					.to_str()
					.ok_or_else(|| invalid("a path is not UTF-8"))?
					.to_string();
			}
			Component::RootDir => rooted = true,
			Component::CurDir => {}
			Component::ParentDir => {
				if matches!(segments.last(), Some(last) if *last != "..") {
					segments.pop();
				} else if !rooted {
					segments.push("..");
				}
			}
			Component::Normal(name) => {
				segments.push(name.to_str().ok_or_else(|| invalid("a path is not UTF-8"))?);
			}
		}
	}
	let mut key = prefix;
	if rooted {
		key.push('/');
	}
	key.push_str(&segments.join("/"));
	if key.contains('\0') {
		return Err(invalid("a path has a NUL in it"));
	}
	Ok(key)
}

/// The directory that holds `key`, if it has one.
fn parent_of(key: &str) -> Option<&str> {
	if key.is_empty() || key == "/" {
		return None;
	}
	match key.rfind('/') {
		None => Some(""),
		Some(0) => Some("/"),
		Some(at) => Some(&key[..at]),
	}
}

/// Every directory above `key`, shallowest first, and `key` itself if `include_self`.
fn ancestors_of(key: &str, include_self: bool) -> Vec<&str> {
	let mut out: Vec<&str> = key
		.match_indices('/')
		.map(|(at, _)| at)
		.filter(|at| *at > 0)
		.map(|at| &key[..at])
		.collect();
	if include_self && !key.is_empty() && key != "/" {
		out.push(key);
	}
	out
}

/// What a name must be to be a file: not the root, and short enough for a header.
fn check_file_key(key: &str) -> io::Result<()> {
	if key.is_empty() || key == "/" {
		return Err(invalid("a directory is not a file"));
	}
	if key.len() > MAX_PATH_LEN {
		return Err(invalid("the path is longer than a pool holds"));
	}
	Ok(())
}

/// The prefix of the names below `dir`.
fn below(dir: &str) -> String {
	if dir.is_empty() || dir.ends_with('/') {
		dir.to_string()
	} else {
		format!("{dir}/")
	}
}

fn slot_name(index: u64) -> String {
	format!("p{index:08x}")
}

/// The number in `p[0-9a-f]{8}`, and nothing for any other name.
fn parse_slot_name(name: &str) -> Option<u64> {
	let digits = name.strip_prefix('p')?;
	if digits.len() != 8
		|| !digits.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
	{
		return None;
	}
	u64::from_str_radix(digits, 16).ok()
}

// ---------------------------------------------------------------------------------------------
// Helpers over a raw file
// ---------------------------------------------------------------------------------------------

/// Writes all of `buf` at `offset`, looping over short writes, and says how many bytes landed even
/// when it fails: a write that makes no progress is `WriteZero`.
fn write_fully<F: RawFile>(raw: &F, offset: u64, buf: &[u8]) -> (usize, io::Result<()>) {
	let mut done = 0usize;
	while done < buf.len() {
		let Some(at) = offset.checked_add(done as u64) else {
			return (done, Err(invalid("a write overflows the offset")));
		};
		match raw.write_at(at, &buf[done..]) {
			Ok(0) => {
				return (
					done,
					Err(io::Error::new(io::ErrorKind::WriteZero, "a write made no progress")),
				);
			}
			Ok(n) => done += n.min(buf.len() - done),
			Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
			Err(e) => return (done, Err(e)),
		}
	}
	(done, Ok(()))
}

/// Fills `buf` from `offset` as far as the file goes, and says how far that is.
fn read_fully<F: RawFile>(raw: &F, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
	let mut done = 0usize;
	while done < buf.len() {
		let Some(at) = offset.checked_add(done as u64) else {
			break;
		};
		match raw.read_at(at, &mut buf[done..]) {
			Ok(0) => break,
			Ok(n) => done += n.min(buf.len() - done),
			Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
			Err(e) => return Err(e),
		}
	}
	Ok(done)
}

/// Polls a future until it is done, with a waker that does nothing, and says nothing if it does not
/// complete. For a backend whose futures complete without waiting for anything outside the
/// process (the one in memory): this is what lets the in-memory file system be made, and mounted
/// again after a simulated crash, without a runtime.
pub(crate) fn poll_ready<F: Future>(future: F) -> Option<F::Output> {
	let mut future = Box::pin(future);
	let mut cx = Context::from_waker(Waker::noop());
	for _ in 0..1_000_000 {
		if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
			return Some(output);
		}
	}
	None
}

/// Polls every future until each is done, and gives the results in order. Nothing is dropped
/// before it completes, which is what a browser needs of opens that are in flight: a handle that
/// arrives after its future was dropped stays locked.
struct JoinAllSettled<'a, T> {
	futures: Vec<Option<RawFut<'a, T>>>,
	results: Vec<Option<T>>,
}

impl<'a, T> JoinAllSettled<'a, T> {
	fn new(futures: Vec<RawFut<'a, T>>) -> Self {
		let results = futures.iter().map(|_| None).collect();
		Self {
			futures: futures.into_iter().map(Some).collect(),
			results,
		}
	}
}

impl<T> Unpin for JoinAllSettled<'_, T> {}

impl<T> Future for JoinAllSettled<'_, T> {
	type Output = Vec<T>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<T>> {
		let this = &mut *self;
		let mut pending = false;
		for (slot, result) in this.futures.iter_mut().zip(this.results.iter_mut()) {
			if let Some(future) = slot {
				match future.as_mut().poll(cx) {
					Poll::Ready(value) => {
						*result = Some(value);
						*slot = None;
					}
					Poll::Pending => pending = true,
				}
			}
		}
		if pending {
			return Poll::Pending;
		}
		Poll::Ready(this.results.iter_mut().filter_map(Option::take).collect())
	}
}

// ---------------------------------------------------------------------------------------------
// Slots
// ---------------------------------------------------------------------------------------------

/// What a slot is, as the pool knows it. Everything here is under the `st` lock of the slot.
struct SlotInfo {
	/// The name in the header that counts, if it is a named slot.
	path: Option<String>,
	seq: u64,
	/// Which page holds the header that counts.
	active_page: usize,
	/// The number of header writes, and how many of them a flush has covered.
	hdr_gen: u64,
	flushed_gen: u64,
	/// The number of the last write of a named header.
	named_gen: u64,
	/// Open files on this slot.
	views: usize,
	/// No name points at it any more.
	unlinked: bool,
	/// It is on the dirty list, or is being cleaned.
	queued: bool,
	/// A manifest write that failed after its commit point left it in a state that is not known:
	/// it is never reused.
	poisoned: bool,
	/// Kept at the mount, never used.
	orphan: bool,
}

impl SlotInfo {
	fn fresh(active_page: usize, seq: u64) -> Self {
		Self {
			path: None,
			seq,
			active_page,
			hdr_gen: 0,
			flushed_gen: 0,
			named_gen: 0,
			views: 0,
			unlinked: false,
			queued: false,
			poisoned: false,
			orphan: false,
		}
	}
}

struct Slot<R: Raw> {
	name: String,
	raw: R::File,
	st: Mutex<SlotInfo>,
	/// The logical length: the bytes after the headers. Written under `st`, read without it.
	end: AtomicU64,
}

impl<R: Raw> Slot<R> {
	fn new(name: String, raw: R::File, info: SlotInfo, end: u64) -> Arc<Self> {
		Arc::new(Self {
			name,
			raw,
			st: Mutex::new(info),
			end: AtomicU64::new(end),
		})
	}

	/// After a write or a truncation that failed: the length is the one the file has, if it can
	/// say, and `fallback` if it cannot or says something that cannot be.
	fn resync_end(&self, fallback: u64) {
		let end = match self.raw.size() {
			Ok(size) if size >= HEADER_LEN => size - HEADER_LEN,
			_ => fallback,
		};
		self.end.store(end, Ordering::Release);
	}

	/// An open file on the slot.
	fn view(self: &Arc<Self>, pool: &Arc<PoolShared<R>>) -> Arc<PoolFile<R>> {
		self.st.lock().views += 1;
		Arc::new(PoolFile {
			slot: Arc::clone(self),
			pool: Arc::downgrade(pool),
		})
	}
}

struct Namespace<R: Raw> {
	files: BTreeMap<String, Arc<Slot<R>>>,
	dirs: BTreeSet<String>,
}

impl<R: Raw> Namespace<R> {
	fn new() -> Self {
		let mut dirs = BTreeSet::new();
		// The roots of the two kinds of path always exist.
		dirs.insert(String::new());
		dirs.insert("/".to_string());
		Self {
			files: BTreeMap::new(),
			dirs,
		}
	}

	fn is_dir(&self, key: &str) -> bool {
		self.dirs.contains(key)
	}

	/// Whether the directory that would hold `key` is there.
	fn parent_exists(&self, key: &str) -> bool {
		parent_of(key).is_none_or(|parent| self.dirs.contains(parent))
	}

	fn entries(&self, dir: &str) -> Vec<DirEntry> {
		let prefix = below(dir);
		let mut out = Vec::new();
		let direct = |rest: &str| !rest.is_empty() && !rest.contains('/');
		for (key, _) in self.files.range(prefix.clone()..) {
			let Some(rest) = key.strip_prefix(prefix.as_str()) else {
				break;
			};
			if direct(rest) {
				out.push(DirEntry {
					name: rest.into(),
					kind: EntryKind::File,
				});
			}
		}
		for key in self.dirs.range(prefix.clone()..) {
			let Some(rest) = key.strip_prefix(prefix.as_str()) else {
				break;
			};
			if direct(rest) {
				out.push(DirEntry {
					name: rest.into(),
					kind: EntryKind::Dir,
				});
			}
		}
		out
	}
}

// ---------------------------------------------------------------------------------------------
// The shared state of a pool
// ---------------------------------------------------------------------------------------------

struct Free<R: Raw> {
	clean: Vec<Arc<Slot<R>>>,
	dirty: Vec<Arc<Slot<R>>>,
}

pub(crate) struct PoolShared<R: Raw> {
	raw: R,
	cfg: PoolConfig,
	me: Weak<PoolShared<R>>,
	ns: Mutex<Namespace<R>>,
	/// The free lists: a leaf lock.
	free: Mutex<Free<R>>,
	/// Every slot that this mount holds a handle of, for closing and reporting.
	slots: Mutex<Vec<Arc<Slot<R>>>>,
	/// Slots in `free`: clean and dirty.
	avail: AtomicUsize,
	/// Slots that exist or are being made, against `max_files`.
	total: AtomicUsize,
	next_phys: AtomicU64,
	seq: AtomicU64,
	reserve: AtomicUsize,
	exhaustions: AtomicU64,
	/// How many free slots the refill task was asked for.
	want: AtomicUsize,
	refill: Arc<Notify>,
	refilled: Arc<Notify>,
	closed: AtomicBool,
	bg_alive: AtomicBool,
	refill_failures: AtomicUsize,
	/// The number of times a refill has failed for good, and the last such error.
	refill_error: Mutex<(u64, Option<(io::ErrorKind, String)>)>,
	clean_failures: AtomicU64,
	locks: Arc<LockRegistry>,
	lock_handle: Mutex<Option<R::File>>,
	report: Mutex<Option<MountReport>>,
}

impl<R: Raw> PoolShared<R> {
	fn next_seq(&self) -> u64 {
		self.seq.fetch_add(1, Ordering::AcqRel) + 1
	}

	/// How many slots can be had without waiting: the free ones, and, where the backend makes
	/// them on the spot, the ones it could still make.
	fn available(&self) -> usize {
		let free = self.avail.load(Ordering::Acquire);
		if self.cfg.inline_provision && self.raw.can_create_sync() {
			return free.saturating_add(self.headroom());
		}
		free
	}

	/// Slots that the cap still allows.
	fn headroom(&self) -> usize {
		self.cfg.max_files.saturating_sub(self.total.load(Ordering::Acquire))
	}

	/// Free slots, and the ones that the cap lets be made: the most that a wait could ever get.
	fn potential(&self) -> usize {
		self.avail.load(Ordering::Acquire).saturating_add(self.headroom())
	}

	fn live(&self) -> io::Result<()> {
		if self.closed.load(Ordering::Acquire) {
			return Err(io::Error::new(io::ErrorKind::NotConnected, "the file system is closed"));
		}
		Ok(())
	}

	// ---- headers ----------------------------------------------------------------------------

	/// Writes a header into the page of the slot that does not count. Nothing changes if the write
	/// fails, so the old header still counts, and a retry writes the same page again.
	fn write_header(&self, slot: &Slot<R>, state: HeaderState) -> io::Result<u64> {
		// The number is taken under the lock of the slot: two writers of one slot would otherwise
		// be able to put the lower number in the page that is written second, and the page that
		// counts at a mount would not be the one that memory says counts.
		let mut st = slot.st.lock();
		let seq = self.next_seq();
		let page = Header {
			seq,
			state: state.clone(),
		}
		.encode()?;
		let target = 1 - st.active_page;
		let (_, written) = write_fully(&slot.raw, page_offset(target), &page);
		written?;
		st.active_page = target;
		st.seq = seq;
		st.hdr_gen += 1;
		match state {
			HeaderState::Named(path) => {
				st.named_gen = st.hdr_gen;
				st.path = Some(path);
			}
			HeaderState::Free => st.path = None,
		}
		Ok(seq)
	}

	/// Flushes a slot, and with it every header write that came before.
	fn flush_slot(&self, slot: &Slot<R>) -> io::Result<()> {
		let generation = slot.st.lock().hdr_gen;
		slot.raw.flush()?;
		let mut st = slot.st.lock();
		st.flushed_gen = st.flushed_gen.max(generation);
		Ok(())
	}

	/// Whether a named header of the slot is not flushed.
	fn named_unflushed(slot: &Slot<R>) -> bool {
		let st = slot.st.lock();
		st.named_gen > st.flushed_gen
	}

	/// Writes the headers of a slot that is new: both pages free.
	fn init_blank(&self, raw: &R::File) -> io::Result<()> {
		let page = Header::free(0).encode()?;
		write_fully(raw, page_offset(0), &page).1?;
		write_fully(raw, page_offset(1), &page).1?;
		raw.flush()
	}

	// ---- the free lists ---------------------------------------------------------------------

	fn push_dirty(&self, slot: Arc<Slot<R>>) {
		self.free.lock().dirty.push(slot);
		self.avail.fetch_add(1, Ordering::AcqRel);
		// A waiter for spares can use it: it is claimable at once.
		self.refilled.notify_waiters();
	}

	/// Gives a slot that was claimed and not used back, as dead.
	fn discard(&self, slot: Arc<Slot<R>>) {
		// The header may have been written, whether or not the write was reported: make sure the
		// slot does not name anything that is not there. If this fails too, the clean will do it.
		let _ = self.write_header(&slot, HeaderState::Free);
		{
			let mut st = slot.st.lock();
			st.unlinked = true;
			st.queued = true;
		}
		self.push_dirty(slot);
	}

	/// The name of `slot` is gone. If nothing has it open it is dead now, and if something has,
	/// it is dead when that is closed.
	fn unlink(&self, slot: &Arc<Slot<R>>) {
		let queue = {
			let mut st = slot.st.lock();
			st.unlinked = true;
			st.path = None;
			if st.views == 0 && !st.queued && !st.poisoned {
				st.queued = true;
				true
			} else {
				false
			}
		};
		if queue {
			self.push_dirty(Arc::clone(slot));
		}
	}

	/// Makes a dead slot a clean one: cut to the headers, both pages free, flushed. The truncation
	/// comes first: a crash after it and before the headers leaves the name of a dead file with no
	/// bytes, which is no worse than the dead file, and loses to any newer claim of the path.
	fn clean_slot(&self, slot: &Slot<R>) -> io::Result<()> {
		let seq = self.next_seq();
		slot.raw.set_len(HEADER_LEN)?;
		let page = Header::free(seq).encode()?;
		write_fully(&slot.raw, page_offset(0), &page).1?;
		write_fully(&slot.raw, page_offset(1), &page).1?;
		slot.raw.flush()?;
		let mut st = slot.st.lock();
		*st = SlotInfo::fresh(0, seq);
		drop(st);
		slot.end.store(0, Ordering::Release);
		Ok(())
	}

	/// Cleans up to `budget` dead slots and makes them claimable without any I/O.
	fn maintain(&self, budget: usize) -> usize {
		let mut cleaned = 0;
		let mut failed = Vec::new();
		while cleaned < budget {
			let Some(slot) = self.free.lock().dirty.pop() else {
				break;
			};
			match self.clean_slot(&slot) {
				Ok(()) => {
					self.free.lock().clean.push(slot);
					cleaned += 1;
				}
				Err(e) => {
					self.clean_failures.fetch_add(1, Ordering::Relaxed);
					tracing::warn!("cleaning slot {} failed: {e}", slot.name);
					failed.push(slot);
				}
			}
		}
		if !failed.is_empty() {
			self.free.lock().dirty.extend(failed);
		}
		cleaned
	}

	/// A slot to make a file of: clean, with no name. A dead slot is cleaned for it if there is no
	/// clean one.
	fn claim(&self, class: SpareClass) -> io::Result<Arc<Slot<R>>> {
		self.live()?;
		if class == SpareClass::Opportunistic
			&& self.available() <= self.reserve.load(Ordering::Acquire)
		{
			self.exhaustions.fetch_add(1, Ordering::AcqRel);
			self.kick();
			return Err(pool_exhausted());
		}
		let mut failed = Vec::new();
		let mut last_error = None;
		let found = loop {
			let popped = {
				let mut free = self.free.lock();
				match free.clean.pop() {
					Some(slot) => Some((slot, false)),
					None => free.dirty.pop().map(|slot| (slot, true)),
				}
			};
			match popped {
				Some((slot, false)) => {
					self.avail.fetch_sub(1, Ordering::AcqRel);
					break Some(slot);
				}
				Some((slot, true)) => {
					self.avail.fetch_sub(1, Ordering::AcqRel);
					match self.clean_slot(&slot) {
						Ok(()) => break Some(slot),
						Err(e) => {
							self.clean_failures.fetch_add(1, Ordering::Relaxed);
							last_error = Some(e);
							failed.push(slot);
						}
					}
				}
				None => break None,
			}
		};
		if !failed.is_empty() {
			let count = failed.len();
			self.free.lock().dirty.extend(failed);
			self.avail.fetch_add(count, Ordering::AcqRel);
		}
		if let Some(slot) = found {
			if self.avail.load(Ordering::Acquire) < self.cfg.low {
				self.kick();
			}
			return Ok(slot);
		}
		if self.cfg.inline_provision && self.raw.can_create_sync() {
			match self.create_inline() {
				Ok(Some(slot)) => return Ok(slot),
				Ok(None) => {}
				Err(e) => return Err(e),
			}
		}
		self.exhaustions.fetch_add(1, Ordering::AcqRel);
		self.kick();
		if let Some(e) = last_error {
			return Err(e);
		}
		if self.total.load(Ordering::Acquire) >= self.cfg.max_files {
			Err(pool_full(self.cfg.max_files))
		} else {
			Err(pool_exhausted())
		}
	}

	/// Makes a slot on the spot, if the backend can and the cap allows.
	fn create_inline(&self) -> io::Result<Option<Arc<Slot<R>>>> {
		if !self.reserve_slot() {
			return Ok(None);
		}
		let name = slot_name(self.next_phys.fetch_add(1, Ordering::AcqRel));
		let raw = match self.raw.create_sync(&name) {
			Some(Ok(raw)) => raw,
			Some(Err(e)) => {
				self.total.fetch_sub(1, Ordering::AcqRel);
				return Err(e);
			}
			None => {
				self.total.fetch_sub(1, Ordering::AcqRel);
				return Ok(None);
			}
		};
		if let Err(e) = self.init_blank(&raw) {
			let _ = raw.close();
			self.total.fetch_sub(1, Ordering::AcqRel);
			return Err(e);
		}
		let slot = Slot::new(name, raw, SlotInfo::fresh(0, 0), 0);
		self.slots.lock().push(Arc::clone(&slot));
		Ok(Some(slot))
	}

	/// Counts one more slot against the cap, if there is room.
	fn reserve_slot(&self) -> bool {
		let mut total = self.total.load(Ordering::Acquire);
		loop {
			if total >= self.cfg.max_files {
				return false;
			}
			match self.total.compare_exchange_weak(
				total,
				total + 1,
				Ordering::AcqRel,
				Ordering::Acquire,
			) {
				Ok(_) => return true,
				Err(now) => total = now,
			}
		}
	}

	// ---- refilling --------------------------------------------------------------------------

	/// Asks for a refill, to the level that the configuration says.
	fn kick(&self) {
		if self.cfg.high == 0 && !self.cfg.background_refill {
			return;
		}
		self.want.fetch_max(self.cfg.high, Ordering::AcqRel);
		self.refill.notify_one();
		self.ensure_background();
	}

	/// Starts the task that refills, if one is wanted, there is a runtime to run it, and none runs.
	fn ensure_background(&self) {
		if !self.cfg.background_refill
			|| self.closed.load(Ordering::Acquire)
			|| self.bg_alive.swap(true, Ordering::AcqRel)
		{
			return;
		}
		let spawned = match self.me.upgrade() {
			Some(me) => {
				let weak = Arc::downgrade(&me);
				let refill = Arc::clone(&self.refill);
				drop(me);
				// The guard is made here and moved into the task: a task that is dropped before it
				// is polled, with the runtime that was to run it, still clears the flag.
				let guard = AliveGuard(Weak::clone(&weak));
				rt::try_spawn(async move {
					let _guard = guard;
					background_refill(weak, refill).await;
				})
			}
			None => false,
		};
		if !spawned {
			self.bg_alive.store(false, Ordering::Release);
		}
	}

	/// Makes blank slots until `target` are free, after cleaning the dead ones. Stops with an error
	/// at the cap, or when the backend fails.
	///
	/// Cancel-safe at its awaits as far as the backend is: a creation that is in flight is
	/// awaited to its end, and a slot that arrives after the pool was closed is closed.
	async fn refill_now(&self, target: usize) -> io::Result<()> {
		self.maintain(usize::MAX);
		let mut progressed = false;
		loop {
			self.live()?;
			let have = self.avail.load(Ordering::Acquire);
			// What the cap allows is the most that can be had.
			let goal = target.min(have.saturating_add(self.headroom()));
			if have >= goal {
				return if have >= target {
					Ok(())
				} else {
					// Short of the target and not allowed to make more.
					Err(pool_full(self.cfg.max_files))
				};
			}
			let wanted = (goal - have).min(REFILL_BATCH);
			let mut reserved = 0;
			while reserved < wanted && self.reserve_slot() {
				reserved += 1;
			}
			if reserved == 0 {
				// Somebody else took the last of the room.
				return if progressed {
					Ok(())
				} else {
					Err(pool_full(self.cfg.max_files))
				};
			}
			let names: Vec<String> = (0..reserved)
				.map(|_| slot_name(self.next_phys.fetch_add(1, Ordering::AcqRel)))
				.collect();
			let futures: Vec<_> = names.iter().map(|name| self.raw.create(name)).collect();
			let results = JoinAllSettled::new(futures).await;
			let mut first_error = None;
			for (name, result) in names.into_iter().zip(results) {
				match result {
					Ok(raw) => {
						if self.closed.load(Ordering::Acquire) {
							// Arrived after the pool was closed or abandoned: nobody will close it
							// but this.
							let _ = raw.close();
							self.total.fetch_sub(1, Ordering::AcqRel);
							first_error.get_or_insert_with(|| {
								io::Error::new(
									io::ErrorKind::NotConnected,
									"the file system is closed",
								)
							});
							continue;
						}
						match self.init_blank(&raw) {
							Ok(()) => {
								let slot = Slot::new(name, raw, SlotInfo::fresh(0, 0), 0);
								self.slots.lock().push(Arc::clone(&slot));
								self.free.lock().clean.push(slot);
								self.avail.fetch_add(1, Ordering::AcqRel);
								progressed = true;
							}
							Err(e) => {
								let _ = raw.close();
								self.total.fetch_sub(1, Ordering::AcqRel);
								first_error.get_or_insert(e);
							}
						}
					}
					Err(e) => {
						self.total.fetch_sub(1, Ordering::AcqRel);
						first_error.get_or_insert(e);
					}
				}
			}
			self.refilled.notify_waiters();
			if let Some(e) = first_error {
				return Err(e);
			}
		}
	}

	fn note_refill(&self, result: io::Result<()>) {
		match result {
			Ok(()) => self.refill_failures.store(0, Ordering::Release),
			Err(e) => {
				tracing::warn!("refilling the pool failed: {e}");
				if self.refill_failures.fetch_add(1, Ordering::AcqRel) + 1 >= 3 {
					self.refill_failures.store(0, Ordering::Release);
					let mut error = self.refill_error.lock();
					error.0 += 1;
					error.1 = Some((e.kind(), e.to_string()));
				}
			}
		}
	}

	/// How many times a refill has failed for good so far.
	fn refill_error_count(&self) -> u64 {
		self.refill_error.lock().0
	}

	/// The error of a refill that failed for good after `seen` such failures were counted: every
	/// wait that began before it hears of it, and a wait that begins later waits for the next
	/// try, which it asks for.
	fn refill_error_since(&self, seen: u64) -> Option<io::Error> {
		let error = self.refill_error.lock();
		if error.0 == seen {
			return None;
		}
		error.1.as_ref().map(|(kind, message)| io::Error::new(*kind, message.clone()))
	}

	// ---- closing ----------------------------------------------------------------------------

	/// Closes every handle, without flushing anything.
	fn release_handles(&self) {
		for slot in self.slots.lock().iter() {
			let _ = slot.raw.close();
		}
		if let Some(lock) = self.lock_handle.lock().take() {
			let _ = lock.close();
		}
	}

	fn wake_everyone(&self) {
		self.refilled.notify_waiters();
		self.refill.notify_one();
	}

	fn abandon(&self) {
		self.closed.store(true, Ordering::Release);
		self.release_handles();
		self.wake_everyone();
	}

	fn close(&self) -> io::Result<()> {
		if self.closed.swap(true, Ordering::AcqRel) {
			return Ok(());
		}
		let slots: Vec<Arc<Slot<R>>> = self.slots.lock().clone();
		let mut first_error = None;
		for slot in &slots {
			let unflushed = {
				let st = slot.st.lock();
				st.hdr_gen > st.flushed_gen
			};
			if unflushed {
				if let Err(e) = self.flush_slot(slot) {
					first_error.get_or_insert(e);
				}
			}
		}
		self.maintain(usize::MAX);
		self.release_handles();
		self.wake_everyone();
		match first_error {
			Some(e) => Err(e),
			None => Ok(()),
		}
	}

	fn stats(&self) -> PoolStats {
		let (clean, dirty) = {
			let free = self.free.lock();
			(free.clean.len(), free.dirty.len())
		};
		let files = self.ns.lock().files.len();
		let mut stats = PoolStats {
			files,
			clean,
			dirty,
			total: self.total.load(Ordering::Acquire),
			max_files: self.cfg.max_files,
			exhaustions: self.exhaustions.load(Ordering::Acquire),
			..PoolStats::default()
		};
		for slot in self.slots.lock().iter() {
			let st = slot.st.lock();
			if st.orphan {
				stats.orphans += 1;
			}
			if st.poisoned {
				stats.poisoned += 1;
			}
			if st.unlinked && st.views > 0 {
				stats.pinned += 1;
			}
		}
		stats
	}
}

impl<R: Raw> Drop for PoolShared<R> {
	fn drop(&mut self) {
		// The last reference: nothing is left that could use a handle, and a handle that is not
		// closed stays locked in a browser.
		self.release_handles();
		self.refill.notify_one();
		self.refilled.notify_waiters();
	}
}

/// Clears the flag of the refill task when the task is gone, whichever way it went.
struct AliveGuard<R: Raw>(Weak<PoolShared<R>>);

impl<R: Raw> Drop for AliveGuard<R> {
	fn drop(&mut self) {
		if let Some(shared) = self.0.upgrade() {
			shared.bg_alive.store(false, Ordering::Release);
		}
	}
}

/// The refill task: sleeps until it is asked, then makes slots until the level that was asked for.
async fn background_refill<R: Raw>(weak: Weak<PoolShared<R>>, refill: Arc<Notify>) {
	loop {
		// Registered before anything is checked, so that a request that comes while the pool is
		// being looked at is not lost.
		let notified = refill.notified();
		tokio::pin!(notified);
		notified.as_mut().enable();
		{
			let Some(shared) = weak.upgrade() else {
				return;
			};
			if shared.closed.load(Ordering::Acquire) {
				return;
			}
			let wanted = shared.want.swap(0, Ordering::AcqRel);
			let target = if shared.avail.load(Ordering::Acquire) < shared.cfg.low {
				wanted.max(shared.cfg.high)
			} else {
				wanted
			};
			// What the cap allows is what is wanted: asking for more is not a failure of the
			// refill.
			let target = target.min(shared.potential());
			if target > 0 && shared.avail.load(Ordering::Acquire) < target {
				let result = shared.refill_now(target).await;
				shared.note_refill(result);
			}
			shared.refilled.notify_waiters();
		}
		notified.await;
	}
}

/// The wait of `Fs::ensure_spares`: until `n` slots can be claimed, the pool is closed, or the
/// refill fails for good. A wait and not a reservation: what it says is true at the poll that
/// returns, and the caller uses it before it awaits anything else.
async fn wait_for_spares<R: Raw>(shared: Arc<PoolShared<R>>, n: usize) -> io::Result<()> {
	let failures_before = shared.refill_error_count();
	loop {
		// Registered before the check: a refill that finishes between the check and the await
		// wakes this wait, and is not missed.
		let notified = shared.refilled.notified();
		tokio::pin!(notified);
		notified.as_mut().enable();
		if shared.available() >= n {
			return Ok(());
		}
		if shared.closed.load(Ordering::Acquire) {
			return Err(io::Error::new(io::ErrorKind::NotConnected, "the file system is closed"));
		}
		if let Some(e) = shared.refill_error_since(failures_before) {
			return Err(e);
		}
		// What the cap allows is all that waiting can bring.
		if shared.potential() < n {
			return Err(pool_full(shared.cfg.max_files));
		}
		let target = n.max(shared.cfg.high);
		shared.want.fetch_max(target, Ordering::AcqRel);
		shared.ensure_background();
		if shared.bg_alive.load(Ordering::Acquire) {
			shared.refill.notify_one();
			notified.await;
			continue;
		}
		// No task to refill: this wait does it, where it can.
		#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
		{
			if let Err(e) = shared.refill_now(target.min(shared.potential())).await {
				// A refill that made enough before it failed is still enough.
				if shared.available() < n {
					return Err(e);
				}
			}
			continue;
		}
		#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
		notified.await;
	}
}

// ---------------------------------------------------------------------------------------------
// The file system
// ---------------------------------------------------------------------------------------------

/// A pool over a backend: the file system of a browser, and, over memory, of a test.
pub(crate) struct PoolFs<R: Raw> {
	shared: Arc<PoolShared<R>>,
}

impl<R: Raw> fmt::Debug for PoolFs<R> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let stats = self.shared.stats();
		f.debug_struct("PoolFs")
			.field("files", &stats.files)
			.field("clean", &stats.clean)
			.field("dirty", &stats.dirty)
			.field("closed", &self.shared.closed.load(Ordering::Acquire))
			.finish()
	}
}

/// How a path is opened.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Open {
	Read,
	Append,
	New,
	Truncate,
}

impl<R: Raw> PoolFs<R> {
	/// Mounts the pool in `backend`: takes the lock, opens every slot, reads every header and
	/// settles what a crash left. Idempotent: a crash inside a mount, and a mount again, ends
	/// where an uninterrupted one would.
	pub(crate) async fn mount(
		backend: R,
		cfg: PoolConfig,
	) -> Result<(Arc<PoolFs<R>>, MountReport)> {
		// 1. The lock, alone and first. Another page releases it when it is closed, or when the
		// browser has killed its worker, which can take a couple of seconds.
		let started = rt::Instant::now();
		let lock = loop {
			match backend.open_lock().await {
				Ok(lock) => break lock,
				Err(e)
					if e.kind() == io::ErrorKind::PermissionDenied
						&& started.elapsed() < cfg.lock_deadline =>
				{
					rt::sleep(cfg.lock_poll).await;
				}
				Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
					return Err(Error::Io(Arc::new(io::Error::new(
						io::ErrorKind::PermissionDenied,
						format!(
							"the database is open in another tab or worker (the page that holds it \
							 may be in a long synchronous operation): {e}"
						),
					))));
				}
				Err(e) => return Err(e.into()),
			}
		};

		// 2. The slots. A failure from here on closes whatever was opened.
		let names = match backend.list().await {
			Ok(names) => names,
			Err(e) => {
				let _ = lock.close();
				return Err(e.into());
			}
		};
		let mut found: Vec<(u64, String)> = Vec::new();
		for name in names {
			match parse_slot_name(&name) {
				Some(index) => found.push((index, name)),
				None => tracing::warn!("the pool ignores {name:?}: it is not a slot"),
			}
		}
		found.sort();
		let empty = found.is_empty();
		let next_phys = found.last().map_or(0, |(index, _)| index + 1);

		// Opened together, and every open is awaited to its end, so that none is dropped in
		// flight: a handle that arrives late would stay locked.
		let mut opened: Vec<Arc<Slot<R>>> = Vec::with_capacity(found.len());
		let mut open_error = None;
		for batch in found.chunks(OPEN_BATCH) {
			let futures: Vec<_> = batch.iter().map(|(_, name)| backend.open(name)).collect();
			for ((_, name), result) in batch.iter().zip(JoinAllSettled::new(futures).await) {
				match result {
					Ok(raw) => opened.push(Slot::new(name.clone(), raw, SlotInfo::fresh(0, 0), 0)),
					Err(e) => {
						open_error.get_or_insert(e);
					}
				}
			}
			if open_error.is_some() {
				break;
			}
		}
		if let Some(e) = open_error {
			for slot in &opened {
				let _ = slot.raw.close();
			}
			let _ = lock.close();
			return Err(e.into());
		}

		let total = opened.len();
		let shared = Arc::new_cyclic(|me| PoolShared {
			raw: backend,
			me: Weak::clone(me),
			ns: Mutex::new(Namespace::new()),
			free: Mutex::new(Free {
				clean: Vec::new(),
				dirty: Vec::new(),
			}),
			slots: Mutex::new(opened.clone()),
			avail: AtomicUsize::new(0),
			total: AtomicUsize::new(total),
			next_phys: AtomicU64::new(next_phys),
			seq: AtomicU64::new(0),
			reserve: AtomicUsize::new(cfg.reserve),
			exhaustions: AtomicU64::new(0),
			want: AtomicUsize::new(0),
			refill: Arc::new(Notify::new()),
			refilled: Arc::new(Notify::new()),
			closed: AtomicBool::new(false),
			bg_alive: AtomicBool::new(false),
			refill_failures: AtomicUsize::new(0),
			refill_error: Mutex::new((0, None)),
			clean_failures: AtomicU64::new(0),
			locks: Arc::new(LockRegistry::default()),
			lock_handle: Mutex::new(Some(lock)),
			report: Mutex::new(None),
			cfg: cfg.clone(),
		});

		// 3. The headers.
		let mut report = match shared.settle(&opened) {
			Ok(report) => report,
			Err(e) => {
				shared.abandon();
				return Err(e.into());
			}
		};

		// An empty directory gets its first slots.
		if empty && cfg.initial > 0 {
			if let Err(e) = shared.refill_now(cfg.initial).await {
				shared.abandon();
				return Err(e.into());
			}
		}
		report.spares = shared.avail.load(Ordering::Acquire);
		*shared.report.lock() = Some(report.clone());
		if shared.avail.load(Ordering::Acquire) < cfg.low {
			shared.kick();
		} else {
			shared.ensure_background();
		}
		Ok((
			Arc::new(PoolFs {
				shared,
			}),
			report,
		))
	}

	/// Mounts a pool over memory, without a runtime.
	pub(crate) fn mount_sync(backend: R, cfg: PoolConfig) -> Result<(Arc<PoolFs<R>>, MountReport)> {
		poll_ready(Self::mount(backend, cfg)).unwrap_or_else(|| {
			Err(Error::Io(Arc::new(io::Error::other(
				"the mount did not complete without waiting for anything outside the process",
			))))
		})
	}

	/// Cleans up to `budget` dead slots.
	pub(crate) fn maintain(&self, budget: usize) -> usize {
		self.shared.maintain(budget)
	}

	/// Makes blank slots until `target` are free. For a caller that drives the refill itself.
	pub(crate) async fn refill(&self, target: usize) -> io::Result<()> {
		self.shared.refill_now(target).await
	}

	pub(crate) fn stats(&self) -> PoolStats {
		self.shared.stats()
	}

	pub(crate) fn raw(&self) -> &R {
		&self.shared.raw
	}

	/// The spares that a claim of the `Opportunistic` class leaves, raised to `n` if it is lower.
	pub(crate) fn set_reserve(&self, n: usize) {
		self.shared.reserve.fetch_max(n, Ordering::AcqRel);
	}

	/// Whether the refill task is running.
	pub(crate) fn background_alive(&self) -> bool {
		self.shared.bg_alive.load(Ordering::Acquire)
	}
}

impl<R: Raw> PoolShared<R> {
	/// Reads the headers of every slot and settles the pool: which are files, which are free,
	/// which are orphans, who wins a path. Writes only to slots that are not a file: blank ones are
	/// initialised, free ones that have a payload are cut, and the losers of a path are cleaned.
	/// Each of those is idempotent, and none touches the slot that wins.
	fn settle(&self, slots: &[Arc<Slot<R>>]) -> io::Result<MountReport> {
		let mut report = MountReport::default();
		let mut max_seq = 0u64;
		let mut claims: HashMap<String, Vec<(u64, usize)>> = HashMap::new();
		let mut free_slots: Vec<usize> = Vec::new();
		let mut losers: Vec<usize> = Vec::new();
		let mut sizes: Vec<u64> = Vec::with_capacity(slots.len());

		for (index, slot) in slots.iter().enumerate() {
			let size = slot.raw.size()?;
			sizes.push(size);
			if size < HEADER_LEN {
				// Never given a header: made by a refill that did not get as far as its first
				// write. There is nothing in it.
				self.reinitialise(slot, size > 0)?;
				report.blank_initialised += 1;
				free_slots.push(index);
				continue;
			}
			let mut head = vec![0u8; HEADER_BYTES];
			let read = read_fully(&slot.raw, 0, &mut head)?;
			head.truncate(read);
			let headers = SlotHeaders::decode(&head);
			for page in headers.pages.iter().flatten() {
				max_seq = max_seq.max(page.seq);
			}
			match headers.current {
				None if size > HEADER_LEN => {
					// A payload that nothing names: kept, as it is.
					slot.st.lock().orphan = true;
					report.orphans_kept.push(slot.name.clone());
				}
				None => {
					self.reinitialise(slot, false)?;
					report.blank_initialised += 1;
					free_slots.push(index);
				}
				Some((page, header)) => {
					{
						let mut st = slot.st.lock();
						*st = SlotInfo::fresh(page, header.seq);
					}
					match header.state {
						HeaderState::Free => {
							if size > HEADER_LEN {
								slot.raw.set_len(HEADER_LEN)?;
								report.cut_back += 1;
							}
							free_slots.push(index);
						}
						HeaderState::Named(path) => {
							claims.entry(path).or_default().push((header.seq, index));
						}
					}
				}
			}
		}
		report.max_seq = max_seq;
		self.seq.store(max_seq, Ordering::Release);

		// Who has each path: the higher sequence number; the lower slot on a tie, which nothing
		// but damage makes.
		let mut files: BTreeMap<String, Arc<Slot<R>>> = BTreeMap::new();
		for (path, mut claimants) in claims {
			claimants.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
			let (_, winner) = claimants[0];
			for (_, loser) in &claimants[1..] {
				losers.push(*loser);
			}
			let slot = &slots[winner];
			slot.end.store(sizes[winner] - HEADER_LEN, Ordering::Release);
			{
				let mut st = slot.st.lock();
				st.path = Some(path.clone());
			}
			files.insert(path, Arc::clone(slot));
		}
		// A loser that stayed would come back to life if the winner were removed.
		losers.sort_unstable();
		for index in losers {
			self.clean_slot(&slots[index])?;
			report.losers_cleaned += 1;
			free_slots.push(index);
		}
		report.files = files.len();

		{
			let mut ns = self.ns.lock();
			for path in files.keys() {
				for dir in ancestors_of(path, false) {
					ns.dirs.insert(dir.to_string());
				}
				if path.starts_with('/') {
					ns.dirs.insert("/".to_string());
				}
			}
			ns.files = files;
		}
		free_slots.sort_unstable();
		{
			let mut free = self.free.lock();
			for index in &free_slots {
				free.clean.push(Arc::clone(&slots[*index]));
			}
		}
		self.avail.store(free_slots.len(), Ordering::Release);
		Ok(report)
	}

	/// A slot that has no header: both pages free, nothing after them.
	fn reinitialise(&self, slot: &Slot<R>, cut: bool) -> io::Result<()> {
		if cut {
			slot.raw.set_len(0)?;
		}
		self.init_blank(&slot.raw)?;
		*slot.st.lock() = SlotInfo::fresh(0, 0);
		slot.end.store(0, Ordering::Release);
		Ok(())
	}
}

impl<R: Raw> PoolFs<R> {
	fn open_path(&self, path: &Path, mode: Open, class: SpareClass) -> io::Result<Arc<dyn FsFile>> {
		let shared = &self.shared;
		let key = key_of(path)?;
		shared.live()?;
		let mut ns = shared.ns.lock();
		if let Some(slot) = ns.files.get(&key).cloned() {
			match mode {
				Open::Read | Open::Append => return Ok(slot.view(shared)),
				Open::New => {
					return Err(io::Error::new(io::ErrorKind::AlreadyExists, "the file is there"));
				}
				Open::Truncate => {
					// A new empty file takes the name and the old one is unlinked, so a reader of
					// the old one keeps its bytes. The new header is flushed before the old one is
					// freed: a crash must not be able to lose both names.
					let fresh = shared.claim(class)?;
					let named = shared
						.write_header(&fresh, HeaderState::Named(key.clone()))
						.and_then(|_| shared.flush_slot(&fresh));
					if let Err(e) = named {
						shared.discard(fresh);
						return Err(e);
					}
					ns.files.insert(key, Arc::clone(&fresh));
					let _ = shared.write_header(&slot, HeaderState::Free);
					shared.unlink(&slot);
					return Ok(fresh.view(shared));
				}
			}
		}
		if ns.is_dir(&key) {
			return Err(match mode {
				Open::New => io::Error::new(io::ErrorKind::AlreadyExists, "a directory is there"),
				_ => invalid("the path is a directory"),
			});
		}
		if mode == Open::Read {
			return Err(io::Error::new(io::ErrorKind::NotFound, "no such file"));
		}
		check_file_key(&key)?;
		if !ns.parent_exists(&key) {
			return Err(io::Error::new(io::ErrorKind::NotFound, "the directory is not there"));
		}
		let slot = shared.claim(class)?;
		if let Err(e) = shared.write_header(&slot, HeaderState::Named(key.clone())) {
			shared.discard(slot);
			return Err(e);
		}
		ns.files.insert(key, Arc::clone(&slot));
		Ok(slot.view(shared))
	}

	fn do_replace_atomic(&self, path: &Path, content: &[u8]) -> Result<()> {
		let shared = &self.shared;
		let key = key_of(path)?;
		shared.live()?;
		{
			let ns = shared.ns.lock();
			if ns.is_dir(&key) {
				return Err(invalid("the path is a directory").into());
			}
			check_file_key(&key)?;
			if !ns.parent_exists(&key) {
				return Err(
					io::Error::new(io::ErrorKind::NotFound, "the directory is not there").into()
				);
			}
		}
		// 1. A slot to hold the new content.
		let slot = shared.claim(SpareClass::Reserved)?;
		// 2 and 3. The content, flushed, while the header still says free: a crash here leaves a
		// free slot with a payload, which a mount cuts off, and the old content in place.
		let staged = {
			let (written, result) = write_fully(&slot.raw, HEADER_LEN, content);
			slot.end.store(written as u64, Ordering::Release);
			result.and_then(|()| shared.flush_slot(&slot))
		};
		if let Err(e) = staged {
			shared.discard(slot);
			return Err(Error::Io(Arc::new(e)));
		}
		// 4. The commit: the header names the path, and one flush makes it and the content
		// durable together. From here a failure leaves the outcome open.
		let mut ns = shared.ns.lock();
		let committed = shared
			.write_header(&slot, HeaderState::Named(key.clone()))
			.and_then(|_| shared.flush_slot(&slot));
		if let Err(e) = committed {
			// Whether the new content is the manifest is not known, and the slot may be what a
			// mount finds. It is kept as it is, and never reused.
			slot.st.lock().poisoned = true;
			return Err(Error::ManifestWriteUncertain(format!(
				"replacing {}: {e}",
				path.display()
			)));
		}
		// 5. The old slot is dead.
		let old = ns.files.insert(key, slot);
		drop(ns);
		if let Some(old) = old {
			let _ = shared.write_header(&old, HeaderState::Free);
			shared.unlink(&old);
		}
		Ok(())
	}
}

impl<R: Raw> Fs for PoolFs<R> {
	fn kind(&self) -> FsKind {
		FsKind::Pool
	}

	fn check_available(&self) -> io::Result<()> {
		self.shared.live()
	}

	fn entry(&self, path: &Path) -> io::Result<Option<EntryKind>> {
		let key = key_of(path)?;
		let ns = self.shared.ns.lock();
		if ns.files.contains_key(&key) {
			Ok(Some(EntryKind::File))
		} else if ns.is_dir(&key) {
			Ok(Some(EntryKind::Dir))
		} else {
			Ok(None)
		}
	}

	fn create_dir_all(&self, path: &Path) -> io::Result<()> {
		let key = key_of(path)?;
		let mut ns = self.shared.ns.lock();
		let chain = ancestors_of(&key, true);
		if chain.iter().any(|dir| ns.files.contains_key(*dir)) {
			return Err(io::Error::new(io::ErrorKind::AlreadyExists, "a file is in the way"));
		}
		if key.starts_with('/') {
			ns.dirs.insert("/".to_string());
		}
		for dir in chain {
			ns.dirs.insert(dir.to_string());
		}
		Ok(())
	}

	fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntry>> {
		let key = key_of(path)?;
		let ns = self.shared.ns.lock();
		if !ns.is_dir(&key) {
			return Err(io::Error::new(io::ErrorKind::NotFound, "not a directory"));
		}
		Ok(ns.entries(&key))
	}

	fn remove_file(&self, path: &Path) -> io::Result<()> {
		let key = key_of(path)?;
		let mut ns = self.shared.ns.lock();
		let Some(slot) = ns.files.remove(&key) else {
			return Err(if ns.is_dir(&key) {
				invalid("the path is a directory")
			} else {
				io::Error::new(io::ErrorKind::NotFound, "no such file")
			});
		};
		// The free header is not flushed: a crash before the clean may bring the file back, which
		// is what a native file system does without a directory sync. A failed write is not an
		// error: the name is gone, and the clean makes the header agree.
		if let Err(e) = self.shared.write_header(&slot, HeaderState::Free) {
			tracing::debug!("the header of a removed file was not written: {e}");
		}
		self.shared.unlink(&slot);
		Ok(())
	}

	fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
		let key = key_of(path)?;
		let mut ns = self.shared.ns.lock();
		if ns.files.contains_key(&key) {
			return Err(invalid("the path is a file"));
		}
		if !ns.is_dir(&key) {
			return Err(io::Error::new(io::ErrorKind::NotFound, "no such directory"));
		}
		let prefix = below(&key);
		let files: Vec<String> = ns
			.files
			.range(prefix.clone()..)
			.map(|(name, _)| name)
			.take_while(|name| name.starts_with(prefix.as_str()))
			.cloned()
			.collect();
		let dirs: Vec<String> = ns
			.dirs
			.range(prefix.clone()..)
			.take_while(|name| name.starts_with(prefix.as_str()))
			.cloned()
			.collect();
		let mut removed = Vec::new();
		for name in files {
			if let Some(slot) = ns.files.remove(&name) {
				removed.push(slot);
			}
		}
		for dir in dirs {
			ns.dirs.remove(&dir);
		}
		// The roots stay.
		if !key.is_empty() && key != "/" {
			ns.dirs.remove(&key);
		}
		for slot in removed {
			let _ = self.shared.write_header(&slot, HeaderState::Free);
			self.shared.unlink(&slot);
		}
		Ok(())
	}

	fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
		let (a, b) = (key_of(from)?, key_of(to)?);
		let shared = &self.shared;
		shared.live()?;
		let mut ns = shared.ns.lock();
		let Some(slot) = ns.files.get(&a).cloned() else {
			return Err(if ns.is_dir(&a) {
				invalid("a directory is not renamed")
			} else {
				io::Error::new(io::ErrorKind::NotFound, "no such file")
			});
		};
		if a == b {
			return Ok(());
		}
		if ns.is_dir(&b) {
			return Err(invalid("the destination is a directory"));
		}
		check_file_key(&b)?;
		if !ns.parent_exists(&b) {
			return Err(io::Error::new(io::ErrorKind::NotFound, "the directory is not there"));
		}
		// The commit point is the flush of this slot: before it the file is still `a` (a torn page
		// fails its checksum and the old page names `a`), and after it the file is `b`.
		shared.write_header(&slot, HeaderState::Named(b.clone()))?;
		if let Err(e) = shared.flush_slot(&slot) {
			// The header may have landed. Name the old path again, so that what is in memory and
			// what a mount would find are the same as far as can be made so.
			let _ = shared.write_header(&slot, HeaderState::Named(a));
			return Err(e);
		}
		ns.files.remove(&a);
		let old = ns.files.insert(b, slot);
		drop(ns);
		if let Some(old) = old {
			// After the flush: if this free header is lost, the mount sees two claimants of the
			// path, and the one with the higher sequence number wins.
			let _ = shared.write_header(&old, HeaderState::Free);
			shared.unlink(&old);
		}
		Ok(())
	}

	fn sync_dir(&self, path: &Path) -> io::Result<()> {
		let key = key_of(path)?;
		let slots: Vec<Arc<Slot<R>>> = {
			let ns = self.shared.ns.lock();
			if !ns.is_dir(&key) {
				return Err(io::Error::new(io::ErrorKind::NotFound, "no such directory"));
			}
			let prefix = below(&key);
			ns.files
				.range(prefix.clone()..)
				.take_while(|(name, _)| name.starts_with(prefix.as_str()))
				.filter(|(name, _)| !name[prefix.len()..].contains('/'))
				.map(|(_, slot)| Arc::clone(slot))
				.collect()
		};
		// The names that were made since the last flush, and no others: a removal is made durable
		// by the clean, and a file that is already named costs nothing.
		for slot in slots {
			if PoolShared::<R>::named_unflushed(&slot) {
				self.shared.flush_slot(&slot)?;
			}
		}
		Ok(())
	}

	fn open_read(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		self.open_path(path, Open::Read, SpareClass::Reserved)
	}

	fn open_append(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.open_path(path, Open::Append, opts.class)
	}

	fn create_new(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.open_path(path, Open::New, opts.class)
	}

	fn create_truncate(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.open_path(path, Open::Truncate, opts.class)
	}

	fn replace_atomic(&self, path: &Path, content: &[u8]) -> Result<()> {
		self.do_replace_atomic(path, content)
	}

	fn lock_file(&self, dir: &Path) -> LockFile {
		LockFile::registered(Arc::clone(&self.shared.locks), dir)
	}

	fn ensure_spares(&self, n: usize) -> Option<SpareWait> {
		if self.shared.available() >= n {
			return None;
		}
		Some(Box::pin(wait_for_spares(Arc::clone(&self.shared), n)))
	}

	fn pool_exhaustions(&self) -> u64 {
		self.shared.exhaustions.load(Ordering::Acquire)
	}

	fn pool(&self) -> Option<&dyn PoolControl> {
		Some(self)
	}
}

// ---------------------------------------------------------------------------------------------
// An open file
// ---------------------------------------------------------------------------------------------

/// A view of a slot: what `open_*` returns. Every view of a slot shares its end, so two handles on
/// one file see each other's writes.
pub(crate) struct PoolFile<R: Raw> {
	slot: Arc<Slot<R>>,
	pool: Weak<PoolShared<R>>,
}

impl<R: Raw> Drop for PoolFile<R> {
	fn drop(&mut self) {
		// Nothing but bookkeeping: no I/O and no registry. A dead slot is queued, and cleaned by
		// whoever needs it.
		let queue = {
			let mut st = self.slot.st.lock();
			st.views = st.views.saturating_sub(1);
			if st.unlinked && st.views == 0 && !st.queued && !st.poisoned {
				st.queued = true;
				true
			} else {
				false
			}
		};
		if queue {
			if let Some(pool) = self.pool.upgrade() {
				pool.push_dirty(Arc::clone(&self.slot));
			}
		}
	}
}

fn io_error(e: io::Error) -> Error {
	Error::Io(Arc::new(e))
}

impl<R: Raw> File for PoolFile<R> {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		let end = self.slot.end.load(Ordering::Acquire);
		if offset >= end || buf.is_empty() {
			return Ok(0);
		}
		let want = usize::try_from(end - offset).map_or(buf.len(), |left| left.min(buf.len()));
		let at =
			HEADER_LEN.checked_add(offset).ok_or_else(|| io_error(invalid("offset overflows")))?;
		self.slot.raw.read_at(at, &mut buf[..want]).map_err(io_error)
	}

	fn size(&self) -> Result<u64> {
		Ok(self.slot.end.load(Ordering::Acquire))
	}
}

impl<R: Raw> FsFile for PoolFile<R> {
	fn len(&self) -> io::Result<u64> {
		Ok(self.slot.end.load(Ordering::Acquire))
	}

	fn append(&self, buf: &[u8]) -> io::Result<()> {
		if buf.is_empty() {
			return Ok(());
		}
		let _st = self.slot.st.lock();
		let end = self.slot.end.load(Ordering::Acquire);
		let offset = HEADER_LEN
			.checked_add(end)
			.filter(|offset| offset.checked_add(buf.len() as u64).is_some())
			.ok_or_else(|| invalid("the file would be too long"))?;
		let (written, result) = write_fully(&self.slot.raw, offset, buf);
		if result.is_ok() {
			self.slot.end.store(end + written as u64, Ordering::Release);
		} else {
			// The length is what landed, also when the write failed: the log cuts a tail back with
			// `set_len`, and has to be able to see how long it is. A write that failed may have put
			// some of its bytes in the file without saying how many, so the file is asked.
			self.slot.resync_end(end + written as u64);
		}
		result
	}

	fn set_len(&self, len: u64) -> io::Result<()> {
		let _st = self.slot.st.lock();
		let physical =
			HEADER_LEN.checked_add(len).ok_or_else(|| invalid("the length is too large"))?;
		match self.slot.raw.set_len(physical) {
			Ok(()) => {
				self.slot.end.store(len, Ordering::Release);
				Ok(())
			}
			Err(e) => {
				let was = self.slot.end.load(Ordering::Acquire);
				self.slot.resync_end(was);
				Err(e)
			}
		}
	}

	fn sync(&self) -> io::Result<()> {
		let generation = self.slot.st.lock().hdr_gen;
		self.slot.raw.flush()?;
		let mut st = self.slot.st.lock();
		st.flushed_gen = st.flushed_gen.max(generation);
		Ok(())
	}
}

// ---------------------------------------------------------------------------------------------
// What only a pool can do
// ---------------------------------------------------------------------------------------------

/// The operations that a pool has and no other file system does, reached through `Fs::pool`.
pub(crate) trait PoolControl: Send + Sync {
	/// Cleans up to `budget` dead slots; returns how many.
	fn maintain(&self, budget: usize) -> usize;
	/// Raises the spares that an `Opportunistic` creation leaves.
	fn set_reserve(&self, n: usize);
	fn stats(&self) -> PoolStats;
	fn mount_report(&self) -> Option<MountReport>;
	/// Flushes what has to be flushed, cleans, and releases every handle. Later reads and writes
	/// fail with `NotConnected`.
	fn close(&self) -> io::Result<()>;
	/// Releases every handle without flushing anything: a crash, for a test.
	fn abandon(&self);
	/// A pool mounted over what a crash would have left. Memory only.
	fn simulate_crash(&self, mode: CrashMode) -> io::Result<Arc<dyn Fs>>;
	/// Cuts a file to `len` without flushing.
	fn raw_truncate(&self, path: &Path, len: u64) -> io::Result<()>;
}

impl<R: Raw> PoolControl for PoolFs<R> {
	fn maintain(&self, budget: usize) -> usize {
		self.shared.maintain(budget)
	}

	fn set_reserve(&self, n: usize) {
		PoolFs::set_reserve(self, n);
	}

	fn stats(&self) -> PoolStats {
		self.shared.stats()
	}

	fn mount_report(&self) -> Option<MountReport> {
		self.shared.report.lock().clone()
	}

	fn close(&self) -> io::Result<()> {
		self.shared.close()
	}

	fn abandon(&self) {
		self.shared.abandon();
	}

	fn simulate_crash(&self, mode: CrashMode) -> io::Result<Arc<dyn Fs>> {
		let image = self.shared.raw.crash_image(mode).ok_or_else(|| {
			io::Error::new(
				io::ErrorKind::Unsupported,
				"only the memory file system can simulate a crash",
			)
		})?;
		let mut cfg = self.shared.cfg.clone();
		cfg.lock_deadline = Duration::ZERO;
		let (fs, _) = PoolFs::mount_sync(image, cfg).map_err(into_io_error)?;
		Ok(fs)
	}

	fn raw_truncate(&self, path: &Path, len: u64) -> io::Result<()> {
		let key = key_of(path)?;
		let slot = self
			.shared
			.ns
			.lock()
			.files
			.get(&key)
			.cloned()
			.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))?;
		let _st = slot.st.lock();
		let physical =
			HEADER_LEN.checked_add(len).ok_or_else(|| invalid("the length is too large"))?;
		slot.raw.set_len(physical)?;
		slot.end.store(len, Ordering::Release);
		Ok(())
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use std::cell::RefCell;
	use std::collections::{BTreeMap, BTreeSet};
	use std::rc::Rc;

	use proptest::prelude::*;

	use super::*;
	use crate::vfs::header::{Header, SlotHeaders, HEADER_LEN, PAGE_LEN};
	use crate::vfs::mem::{MemConfig, MemRaw};
	use crate::vfs::{self, contract, FileSystem};

	type Mem = PoolFs<MemRaw>;

	fn p(path: &str) -> &Path {
		Path::new(path)
	}

	fn mounted(raw: &MemRaw, cfg: PoolConfig) -> Arc<Mem> {
		PoolFs::mount_sync(raw.clone(), cfg).expect("mount").0
	}

	/// A pool over memory that makes slots when it is asked for them.
	fn memory_pool() -> (Arc<Mem>, MemRaw) {
		let raw = MemRaw::new();
		(mounted(&raw, PoolConfig::memory()), raw)
	}

	/// A pool with a fixed number of spares and nothing that refills it but its callers.
	fn strict_pool(initial: usize, max_files: usize) -> (Arc<Mem>, MemRaw) {
		let raw = MemRaw::new();
		(mounted(&raw, PoolConfig::strict(initial, max_files)), raw)
	}

	/// Makes a file with this content, synced.
	fn put(fs: &Mem, path: &str, content: &[u8]) -> Arc<dyn FsFile> {
		let file = fs.create_truncate(p(path), CreateOpts::default()).expect("create_truncate");
		file.append(content).expect("append");
		file.sync().expect("sync");
		file
	}

	fn read(fs: &Mem, path: &str) -> Option<Vec<u8>> {
		fs.read(p(path)).ok()
	}

	fn read_all(file: &Arc<dyn FsFile>) -> Vec<u8> {
		let mut buf = vec![0u8; file.len().unwrap() as usize];
		file.read_exact_at(0, &mut buf).unwrap();
		buf
	}

	fn walk(fs: &Mem, dir: &str, out: &mut BTreeMap<String, Vec<u8>>) {
		let Ok(entries) = fs.read_dir(p(dir)) else {
			return;
		};
		for entry in entries {
			let name = entry.name.to_string_lossy().into_owned();
			let path = if dir.ends_with('/') {
				format!("{dir}{name}")
			} else if dir.is_empty() {
				name
			} else {
				format!("{dir}/{name}")
			};
			match entry.kind {
				EntryKind::File => {
					out.insert(path.clone(), fs.read(p(&path)).expect("a listed file reads"));
				}
				EntryKind::Dir => walk(fs, &path, out),
			}
		}
	}

	/// Every file of the pool and what is in it.
	fn logical(fs: &Mem) -> BTreeMap<String, Vec<u8>> {
		let mut out = BTreeMap::new();
		walk(fs, "/", &mut out);
		walk(fs, "", &mut out);
		out
	}

	fn headers_of(raw: &MemRaw, name: &str) -> SlotHeaders {
		SlotHeaders::decode(&raw.image(name).expect("the slot is there"))
	}

	/// The bytes of a slot that has these headers and this payload. A page that is `None` is zeros.
	fn slot_bytes(a: Option<Header>, b: Option<Header>, payload: &[u8]) -> Vec<u8> {
		let mut bytes = vec![0u8; HEADER_LEN as usize];
		if let Some(header) = a {
			bytes[..PAGE_LEN].copy_from_slice(&header.encode().unwrap());
		}
		if let Some(header) = b {
			bytes[PAGE_LEN..].copy_from_slice(&header.encode().unwrap());
		}
		bytes.extend_from_slice(payload);
		bytes
	}

	/// What must hold of the slots of any pool: no path is named by two slots. Once a mount has
	/// settled them, a slot that is free has nothing after its headers too (until then a file that
	/// was removed while it is open is free and has its bytes).
	fn assert_sound(raw: &MemRaw, context: &str) {
		assert_slots(raw, context, true);
	}

	fn assert_slots(raw: &MemRaw, context: &str, settled: bool) {
		let mut named: BTreeMap<String, String> = BTreeMap::new();
		for name in raw.names() {
			if parse_slot_name(&name).is_none() {
				continue;
			}
			let image = raw.image(&name).unwrap();
			if (image.len() as u64) < HEADER_LEN {
				continue;
			}
			match SlotHeaders::decode(&image).current {
				Some((
					_,
					Header {
						state: HeaderState::Named(path),
						..
					},
				)) => {
					if let Some(other) = named.insert(path.clone(), name.clone()) {
						panic!("{context}: {path} is named by {other} and {name}");
					}
				}
				Some((
					_,
					Header {
						state: HeaderState::Free,
						..
					},
				)) if settled => {
					assert_eq!(
						image.len() as u64,
						HEADER_LEN,
						"{context}: the free slot {name} has a payload"
					);
				}
				Some(_) | None => {}
			}
		}
	}

	// ---- the contract ------------------------------------------------------------------------

	#[test]
	fn the_pool_honours_the_contract() {
		let (fs, _) = memory_pool();
		let root = Path::new("/root");
		fs.create_dir_all(root).unwrap();
		contract::run_all(&*fs, root);
	}

	#[test]
	fn a_bounded_pool_honours_the_contract_too() {
		// The contract needs no more than a few dozen files at once, and a pool that can only
		// reuse the ones it has is the one in a browser.
		let (fs, raw) = strict_pool(64, 64);
		let root = Path::new("/root");
		fs.create_dir_all(root).unwrap();
		contract::run_all(&*fs, root);
		assert_eq!(fs.stats().total, 64, "no slot was made beyond the initial ones");
		assert_slots(&raw, "after the contract", false);
	}

	#[test]
	fn the_pool_honours_what_only_a_pool_promises() {
		let (fs, _) = memory_pool();
		fs.create_dir_all(p("/pool")).unwrap();
		contract::pool_semantics(&*fs, p("/pool"));
		let (fs, raw) = memory_pool();
		fs.create_dir_all(p("/pool")).unwrap();
		contract::failed_append_is_truthful(&*fs, p("/pool"), &|k| raw.write_then_fail(k));
	}

	// ---- naming ------------------------------------------------------------------------------

	#[test]
	fn paths_are_names_by_components() {
		for (given, key) in [
			("a/b", "a/b"),
			("a/./b/", "a/b"),
			("a//b", "a/b"),
			("a/b/..", "a"),
			("a/b/../../c", "c"),
			("../a", "../a"),
			("../../a", "../../a"),
			("/a/../..", "/"),
			("/a/./b", "/a/b"),
			("/", "/"),
			("", ""),
			(".", ""),
		] {
			assert_eq!(key_of(p(given)).unwrap(), key, "{given:?}");
		}
		assert_eq!(key_of(p("a\0b")).unwrap_err().kind(), io::ErrorKind::InvalidInput);
		#[cfg(unix)]
		{
			use std::os::unix::ffi::OsStrExt;
			let bad = Path::new(std::ffi::OsStr::from_bytes(b"a/\xff"));
			assert_eq!(key_of(bad).unwrap_err().kind(), io::ErrorKind::InvalidInput);
		}
		assert_eq!(parent_of("a/b/c"), Some("a/b"));
		assert_eq!(parent_of("a"), Some(""));
		assert_eq!(parent_of("/a"), Some("/"));
		assert_eq!(parent_of("/"), None);
		assert_eq!(parent_of(""), None);
		assert_eq!(ancestors_of("/a/b/c", false), vec!["/a", "/a/b"]);
		assert_eq!(ancestors_of("a/b", true), vec!["a", "a/b"]);
		assert_eq!(parse_slot_name("p0000002a"), Some(0x2a));
		for bad in ["p2a", "q0000002a", "p0000002A", "p0000002g", "p000000000", "LOCK", ""] {
			assert_eq!(parse_slot_name(bad), None, "{bad:?}");
		}
		assert_eq!(slot_name(0x2a), "p0000002a");
	}

	#[test]
	fn paths_that_are_the_same_name_are_one_file() {
		let (fs, _) = memory_pool();
		fs.create_dir_all(p("a/b")).unwrap();
		put(&fs, "a/b/f", b"one");
		assert_eq!(read(&fs, "./a//b/../b/f").unwrap(), b"one");
		assert!(fs.exists(p("a/./b")));
		assert_eq!(fs.read_dir(p("a")).unwrap().len(), 1);
	}

	#[test]
	fn a_path_that_does_not_fit_in_a_header_is_refused_and_creates_nothing() {
		let (fs, raw) = memory_pool();
		let long = format!("/d/{}", "x".repeat(MAX_PATH_LEN));
		fs.create_dir_all(p("/d")).unwrap();
		let before = raw.names().len();
		assert_eq!(
			fs.create_new(p(&long), CreateOpts::default()).err().map(|e| e.kind()),
			Some(io::ErrorKind::InvalidInput)
		);
		assert_eq!(
			fs.replace_atomic(p(&long), b"x").err().map(|e| matches!(e, Error::Io(_))),
			Some(true)
		);
		assert_eq!(raw.names().len(), before, "no slot was claimed for it");
		assert!(!fs.exists(p(&long)));
		// A file in the root has a name of its own: the root is not a file.
		assert_eq!(
			fs.open_append(p("/"), CreateOpts::default()).err().map(|e| e.kind()),
			Some(io::ErrorKind::InvalidInput)
		);
	}

	// ---- cost --------------------------------------------------------------------------------

	#[test]
	fn what_each_operation_costs_in_flushes() {
		let (fs, raw) = strict_pool(8, 64);
		fs.create_dir_all(p("/d")).unwrap();
		let flushes = |f: &dyn Fn()| {
			let before = raw.flushes();
			f();
			raw.flushes() - before
		};
		let mut created = None;
		assert_eq!(
			flushes(&|| {
				let _ = fs.create_new(p("/d/a"), CreateOpts::default()).unwrap();
			}),
			0,
			"a creation writes a header and flushes nothing"
		);
		let file = fs.open_append(p("/d/a"), CreateOpts::default()).unwrap();
		created.replace(Arc::clone(&file));
		assert_eq!(flushes(&|| file.append(b"abc").unwrap()), 0);
		assert_eq!(flushes(&|| file.sync().unwrap()), 1, "a sync is one flush");
		assert_eq!(flushes(&|| file.sync().unwrap()), 1, "and a flush is asked for again");

		let b = fs.create_new(p("/d/b"), CreateOpts::default()).unwrap();
		let c = fs.create_new(p("/d/c"), CreateOpts::default()).unwrap();
		assert_eq!(
			flushes(&|| fs.sync_dir(p("/d")).unwrap()),
			2,
			"the two names that were not flushed"
		);
		assert_eq!(flushes(&|| fs.sync_dir(p("/d")).unwrap()), 0, "and no more");
		b.append(b"x").unwrap();
		assert_eq!(
			flushes(&|| fs.sync_dir(p("/d")).unwrap()),
			0,
			"a file that is named already is not flushed by the directory"
		);

		assert_eq!(
			flushes(&|| fs.rename(p("/d/b"), p("/d/b2")).unwrap()),
			1,
			"a rename is one flush"
		);
		assert_eq!(flushes(&|| fs.remove_file(p("/d/c")).unwrap()), 0, "a removal is none");
		drop(c);
		assert_eq!(
			flushes(&|| fs.replace_atomic(p("/d/M"), b"one").unwrap()),
			2,
			"a replacement is two"
		);
		assert_eq!(flushes(&|| fs.replace_atomic(p("/d/M"), b"two").unwrap()), 2);
		assert_eq!(fs.stats().dirty, 2, "the removed file and the first manifest are dead");
		assert_eq!(flushes(&|| assert_eq!(fs.maintain(1), 1)), 1, "a clean is one flush");
		assert_eq!(flushes(&|| assert_eq!(fs.maintain(10), 1)), 1);
		assert_eq!(flushes(&|| assert_eq!(fs.maintain(10), 0)), 0);
		drop((b, created));
	}

	// ---- the lifecycle of slots --------------------------------------------------------------

	#[test]
	fn an_empty_directory_gets_its_initial_slots() {
		let raw = MemRaw::new();
		let cfg = PoolConfig {
			initial: 5,
			low: 0,
			..PoolConfig::strict(5, 32)
		};
		let (fs, report) = PoolFs::mount_sync(raw.clone(), cfg).unwrap();
		assert_eq!(report.spares, 5);
		assert_eq!(report.files, 0);
		assert_eq!(fs.stats().clean, 5);
		let names = poll_ready(raw.list()).unwrap().unwrap();
		assert_eq!(names.len(), 5);
		assert_eq!(names[0], "p00000000");
		assert_eq!(fs.mount_report(), Some(report));
		for name in names {
			let image = raw.image(&name).unwrap();
			assert_eq!(image.len() as u64, HEADER_LEN);
			assert_eq!(SlotHeaders::decode(&image).current.unwrap().1, Header::free(0));
			assert_eq!(
				raw.durable_image(&name).unwrap(),
				image,
				"the headers of a new slot are flushed"
			);
		}
		assert!(raw.names().contains(&"LOCK".to_string()));
	}

	#[test]
	fn files_survive_a_close_and_a_remount_with_their_directories() {
		let (fs, raw) = strict_pool(4, 64);
		fs.create_dir_all(p("/db/wal")).unwrap();
		fs.create_dir_all(p("/db/sst")).unwrap();
		let log = fs.create_new(p("/db/wal/1.wal"), CreateOpts::default()).unwrap();
		log.append(b"records").unwrap();
		put(&fs, "/db/sst/1.sst", b"table");
		fs.replace_atomic(p("/db/MANIFEST"), b"manifest").unwrap();
		let expected = logical(&fs);
		assert_eq!(expected.len(), 3);
		fs.pool().unwrap().close().unwrap();
		assert_eq!(raw.open_handles(), 0, "every handle was closed");
		assert_eq!(raw.leaked(), 0);
		assert!(log.append(b"more").is_err(), "a file of a closed pool is not connected");
		assert_eq!(log.append(b"more").unwrap_err().kind(), io::ErrorKind::NotConnected);
		assert_eq!(fs.check_available().unwrap_err().kind(), io::ErrorKind::NotConnected);
		assert_eq!(
			fs.open_read(p("/db/MANIFEST")).err().map(|e| e.kind()),
			Some(io::ErrorKind::NotConnected)
		);
		drop(log);
		drop(fs);

		// The same memory, mounted again with nothing in the way: the headers are read back.
		let (fs, report) = PoolFs::mount_sync(raw.clone(), PoolConfig::strict(0, 64)).unwrap();
		assert_eq!(report.files, 3);
		assert!(report.orphans_kept.is_empty());
		assert_eq!(report.losers_cleaned, 0);
		assert_eq!(logical(&fs), expected);
		// The directories are the ancestors of the files that are there.
		assert!(fs.is_dir(p("/db")) && fs.is_dir(p("/db/wal")) && fs.is_dir(p("/db/sst")));
		assert_eq!(fs.read_dir(p("/db")).unwrap().len(), 3);
		assert_eq!(fs.file_len(p("/db/wal/1.wal")).unwrap(), 7);
		// A directory that had no file in it is gone, and a creation in it is `NotFound` until the
		// engine makes it again, as it does at every open.
		assert!(!fs.exists(p("/elsewhere")));
		assert_eq!(
			fs.create_new(p("/elsewhere/f"), CreateOpts::default()).err().map(|e| e.kind()),
			Some(io::ErrorKind::NotFound)
		);
		assert_sound(&raw, "after the remount");
	}

	#[test]
	fn dropping_the_last_reference_closes_every_handle() {
		let (fs, raw) = strict_pool(4, 16);
		assert!(raw.open_handles() >= 5);
		drop(fs);
		assert_eq!(raw.open_handles(), 0);
		assert_eq!(raw.leaked(), 0);
		// So the strict backend, which keeps a leaked file locked, mounts again.
		let _ = mounted(&raw, PoolConfig::strict(0, 16));
	}

	#[test]
	fn a_removed_file_is_cleaned_and_its_slot_is_reused_with_nothing_of_the_old_bytes() {
		let (fs, raw) = strict_pool(1, 8);
		fs.create_dir_all(p("/d")).unwrap();
		let stale = vec![0xEE; 5000];
		put(&fs, "/d/a", &stale);
		assert_eq!(fs.stats().clean, 0);
		fs.remove_file(p("/d/a")).unwrap();
		assert_eq!(fs.stats().dirty, 1);
		assert_eq!(fs.stats().clean, 0);
		// The claim finds nothing clean and cleans the dead slot for itself.
		let b = fs.create_new(p("/d/b"), CreateOpts::default()).unwrap();
		assert_eq!(b.len().unwrap(), 0, "nothing of the old file");
		b.append(b"new").unwrap();
		assert_eq!(fs.stats().total, 1, "no slot was made");
		let image = raw.image("p00000000").unwrap();
		assert_eq!(&image[HEADER_LEN as usize..], b"new");
		assert!(!image.contains(&0xEE), "not a byte of the old file is left");
	}

	#[test]
	fn a_removed_file_that_is_open_is_dead_when_the_last_view_is_dropped() {
		let (fs, _) = strict_pool(4, 8);
		fs.create_dir_all(p("/d")).unwrap();
		let first = put(&fs, "/d/a", b"bytes");
		let second = fs.open_read(p("/d/a")).unwrap();
		fs.remove_file(p("/d/a")).unwrap();
		let stats = fs.stats();
		assert_eq!((stats.dirty, stats.pinned, stats.files), (0, 1, 0), "a handle pins the slot");
		assert_eq!(read_all(&first), b"bytes");
		drop(first);
		assert_eq!(fs.stats().dirty, 0, "one view is left");
		assert_eq!(read_all(&second), b"bytes");
		drop(second);
		let stats = fs.stats();
		assert_eq!((stats.dirty, stats.pinned), (1, 0), "the slot is dead, and counted as free");
		assert_eq!(fs.maintain(8), 1);
		assert_eq!(fs.stats().clean, 4);
	}

	#[test]
	fn a_slot_that_a_snapshot_pins_counts_against_the_cap() {
		let (fs, _) = strict_pool(2, 2);
		fs.create_dir_all(p("/d")).unwrap();
		let pinned = put(&fs, "/d/a", b"a");
		put(&fs, "/d/b", b"b");
		fs.remove_file(p("/d/a")).unwrap();
		fs.remove_file(p("/d/b")).unwrap();
		// "b" had no reader: it is dead and cleaned for the next claim. "a" is held.
		let c = fs.create_new(p("/d/c"), CreateOpts::default()).unwrap();
		let error = fs.create_new(p("/d/d"), CreateOpts::default()).err().unwrap();
		assert_eq!(error.kind(), io::ErrorKind::StorageFull);
		assert!(vfs::is_pool_full(&error) && !vfs::is_pool_exhausted(&error));
		assert_eq!(fs.stats().pinned, 1);
		drop(pinned);
		fs.create_new(p("/d/d"), CreateOpts::default())
			.expect("the slot is free when the reader goes");
		drop(c);
	}

	// ---- exhaustion and the cap --------------------------------------------------------------

	#[test]
	fn a_pool_with_no_free_slot_says_so_in_a_typed_error_and_counts_it() {
		let (fs, _) = strict_pool(2, 8);
		fs.create_dir_all(p("/d")).unwrap();
		let _a = fs.create_new(p("/d/a"), CreateOpts::default()).unwrap();
		let _b = fs.create_new(p("/d/b"), CreateOpts::default()).unwrap();
		assert_eq!(fs.pool_exhaustions(), 0);
		let error = fs.create_new(p("/d/c"), CreateOpts::default()).err().unwrap();
		assert!(vfs::is_pool_exhausted(&error) && !vfs::is_pool_full(&error));
		assert!(matches!(Error::from(error), Error::FilePoolExhausted));
		assert_eq!(fs.pool_exhaustions(), 1);
		assert!(!fs.exists(p("/d/c")), "nothing was made");
		let error = fs.replace_atomic(p("/d/M"), b"m").err().unwrap();
		assert!(matches!(error, Error::FilePoolExhausted), "{error:?}");
		assert_eq!(fs.pool_exhaustions(), 2);

		// Refilling makes it pass: the same call now succeeds.
		poll_ready(fs.refill(4)).unwrap().unwrap();
		fs.create_new(p("/d/c"), CreateOpts::default()).expect("after the refill");
		fs.replace_atomic(p("/d/M"), b"m").expect("after the refill");
	}

	#[test]
	fn the_cap_is_hard_and_has_its_own_error() {
		let (fs, _) = strict_pool(3, 3);
		fs.create_dir_all(p("/d")).unwrap();
		let held: Vec<_> = (0..3)
			.map(|i| fs.create_new(p(&format!("/d/{i}")), CreateOpts::default()).unwrap())
			.collect();
		let error = fs.create_new(p("/d/x"), CreateOpts::default()).err().unwrap();
		assert_eq!(error.kind(), io::ErrorKind::StorageFull);
		assert!(vfs::is_pool_full(&error));
		assert!(!vfs::is_pool_exhausted(&error), "the cap is not the error that retries");
		assert!(!matches!(Error::from(error), Error::FilePoolExhausted));
		// A refill past the cap does not make a slot, and says why.
		let error = poll_ready(fs.refill(10)).unwrap().unwrap_err();
		assert!(vfs::is_pool_full(&error), "{error:?}");
		assert_eq!(fs.stats().total, 3);
		// Nor does a wait for more than the cap allows, which would never end.
		let wait = fs.ensure_spares(1).expect("none is free");
		let error = poll_ready(wait).unwrap().unwrap_err();
		assert!(vfs::is_pool_full(&error), "{error:?}");
		// The round trip through `Error` keeps the kind and the payload.
		let back = vfs::into_io_error(Error::Io(Arc::new(vfs::pool_full(7))));
		assert!(vfs::is_pool_full(&back));
		drop(held);
	}

	#[test]
	fn an_opportunistic_creation_leaves_the_reserve() {
		let raw = MemRaw::new();
		let fs = mounted(
			&raw,
			PoolConfig {
				reserve: 0,
				..PoolConfig::strict(6, 6)
			},
		);
		fs.create_dir_all(p("/d")).unwrap();
		fs.set_reserve(4);
		let opportunistic = CreateOpts::default().opportunistic();
		let mut held = Vec::new();
		// Six are free and four are kept: the first claim leaves five, the second four, and the
		// third would leave three.
		for name in ["a", "b"] {
			held.push(fs.create_new(p(&format!("/d/{name}")), opportunistic).unwrap());
		}
		let error = fs.create_new(p("/d/c"), opportunistic).err().unwrap();
		assert!(vfs::is_pool_exhausted(&error));
		assert!(fs.create_new(p("/d/c"), CreateOpts::default()).is_ok(), "a commit may take them");
		assert!(fs.create_truncate(p("/d/c"), opportunistic).is_err(), "compaction may not");
		fs.set_reserve(1);
		assert_eq!(fs.pool().unwrap().stats().clean, 3, "a reserve that is lower changes nothing");
		drop(held);
	}

	// ---- ensure_spares -----------------------------------------------------------------------

	#[test]
	fn nothing_to_wait_for_when_enough_are_free() {
		let (fs, _) = strict_pool(4, 8);
		assert!(fs.ensure_spares(0).is_none());
		assert!(fs.ensure_spares(4).is_none());
		assert!(fs.ensure_spares(5).is_some());
		let (memory, _) = memory_pool();
		assert!(
			memory.ensure_spares(100).is_none(),
			"a pool that makes slots on the spot never waits"
		);
		assert!(memory.ensure_spares(10_000).is_some(), "but the cap is the cap");
	}

	#[test]
	fn a_wait_with_no_task_refills_by_itself() {
		let (fs, _) = strict_pool(1, 32);
		let wait = fs.ensure_spares(10).unwrap();
		poll_ready(wait).unwrap().expect("the wait refilled");
		assert!(fs.stats().clean >= 10);
	}

	#[tokio::test]
	async fn many_waiters_are_all_woken_by_the_refill_task() {
		let raw = MemRaw::new();
		let cfg = PoolConfig {
			low: 2,
			high: 16,
			initial: 2,
			background_refill: true,
			max_files: 64,
			inline_provision: false,
			..PoolConfig::default()
		};
		let (fs, _) = PoolFs::mount(raw.clone(), cfg).await.unwrap();
		assert!(fs.ensure_spares(2).is_none());
		let mut waiters = Vec::new();
		for _ in 0..32 {
			let fs = Arc::clone(&fs);
			waiters.push(tokio::spawn(async move { fs.ensure_spares(10).unwrap().await }));
		}
		for waiter in waiters {
			tokio::time::timeout(Duration::from_secs(10), waiter)
				.await
				.expect("no waiter is left waiting")
				.unwrap()
				.expect("and none fails");
		}
		assert!(fs.stats().clean >= 10);
		assert!(fs.background_alive(), "the task is what refilled");
		assert_eq!(raw.leaked(), 0);
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn no_wakeup_is_lost_while_claims_and_refills_race() {
		let raw = MemRaw::new();
		let cfg = PoolConfig {
			low: 4,
			high: 12,
			initial: 12,
			background_refill: true,
			max_files: 256,
			inline_provision: false,
			..PoolConfig::default()
		};
		let (fs, _) = PoolFs::mount(raw.clone(), cfg).await.unwrap();
		fs.create_dir_all(p("/d")).unwrap();
		let mut next = 0usize;
		for round in 0..40 {
			// Use up the spares, so that the waiters have something to wait for.
			let mut held = Vec::new();
			let first = next + 1;
			for _ in 0..11 {
				next += 1;
				held.push(fs.create_new(p(&format!("/d/f{next}")), CreateOpts::default()).unwrap());
			}
			let mut waiters = Vec::new();
			for _ in 0..8 {
				let fs = Arc::clone(&fs);
				waiters.push(tokio::spawn(async move {
					match fs.ensure_spares(10) {
						Some(wait) => wait.await,
						None => Ok(()),
					}
				}));
			}
			for waiter in waiters {
				tokio::time::timeout(Duration::from_secs(10), waiter)
					.await
					.unwrap_or_else(|_| panic!("a wakeup was lost in round {round}"))
					.unwrap()
					.unwrap();
			}
			assert!(fs.stats().clean + fs.stats().dirty >= 10);
			// Free them again, so that the pool does not run into its cap.
			for i in first..=next {
				fs.remove_file(p(&format!("/d/f{i}"))).unwrap();
			}
			drop(held);
			fs.maintain(usize::MAX);
		}
	}

	#[tokio::test]
	async fn a_closed_pool_ends_every_wait_with_an_error() {
		let raw = MemRaw::new();
		let cfg = PoolConfig {
			low: 0,
			high: 8,
			initial: 1,
			background_refill: true,
			max_files: 64,
			inline_provision: false,
			..PoolConfig::default()
		};
		let (fs, _) = PoolFs::mount(raw.clone(), cfg).await.unwrap();
		// From now on a creation never completes: the waiters wait on the refill.
		raw.set_pending_polls(usize::MAX);
		let mut waiters = Vec::new();
		for _ in 0..4 {
			let fs = Arc::clone(&fs);
			waiters.push(tokio::spawn(async move { fs.ensure_spares(6).unwrap().await }));
		}
		for _ in 0..50 {
			tokio::task::yield_now().await;
		}
		assert!(waiters.iter().all(|waiter| !waiter.is_finished()), "they are waiting");
		fs.pool().unwrap().close().unwrap();
		for waiter in waiters {
			let result = tokio::time::timeout(Duration::from_secs(10), waiter)
				.await
				.expect("close ends the wait")
				.unwrap();
			assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotConnected);
		}
		// And a wait that starts after the close ends at once.
		let error = fs.ensure_spares(6).unwrap().await.unwrap_err();
		assert_eq!(error.kind(), io::ErrorKind::NotConnected);
	}

	#[tokio::test]
	async fn a_refill_that_fails_for_good_ends_the_wait_with_its_error() {
		let raw = MemRaw::new();
		let cfg = PoolConfig {
			low: 0,
			high: 4,
			initial: 0,
			background_refill: true,
			max_files: 64,
			inline_provision: false,
			..PoolConfig::default()
		};
		let (fs, _) = PoolFs::mount(raw.clone(), cfg).await.unwrap();
		raw.quota(Some(0));
		let result = tokio::time::timeout(Duration::from_secs(10), fs.ensure_spares(4).unwrap())
			.await
			.expect("the wait ends");
		assert_eq!(result.unwrap_err().kind(), io::ErrorKind::StorageFull);
		// The quota is lifted and the next wait works.
		raw.quota(None);
		tokio::time::timeout(Duration::from_secs(10), fs.ensure_spares(4).unwrap())
			.await
			.expect("the wait ends")
			.expect("the refill works again");
	}

	// ---- abandon, close, and the handles that are in flight ----------------------------------

	#[test]
	fn a_creation_that_arrives_after_an_abandon_is_closed_by_the_pool() {
		let mut cx = Context::from_waker(Waker::noop());

		let raw = MemRaw::with_config(MemConfig::strict());
		let (fs, _) = PoolFs::mount_sync(raw.clone(), PoolConfig::strict(2, 32)).unwrap();
		let mut refill = Box::pin(fs.refill(8));
		assert!(refill.as_mut().poll(&mut cx).is_pending(), "the creations are in flight");
		let in_flight = raw.open_handles();
		assert!(in_flight > 3, "the files exist and are open: {in_flight}");

		fs.pool().unwrap().abandon();
		match refill.as_mut().poll(&mut cx) {
			Poll::Ready(result) => {
				assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotConnected)
			}
			Poll::Pending => panic!("the creations complete at the second poll"),
		}
		assert_eq!(raw.open_handles(), 0, "the handles that arrived late were closed");
		assert_eq!(raw.leaked(), 0);
		drop(refill);
		drop(fs);
		// A new mount of the same memory does not find a file that is locked.
		let (fs, _) = PoolFs::mount_sync(raw, PoolConfig::strict(0, 32)).expect("remount");
		assert!(fs.stats().clean >= 2);
	}

	#[test]
	fn a_failed_mount_closes_what_it_opened_and_releases_the_lock() {
		let raw = MemRaw::with_config(MemConfig::strict());
		{
			let (fs, _) = PoolFs::mount_sync(raw.clone(), PoolConfig::strict(4, 8)).unwrap();
			fs.pool().unwrap().close().unwrap();
		}
		assert_eq!(raw.open_handles(), 0);
		// Somebody holds one of the slots.
		let held = poll_ready(raw.open("p00000002")).unwrap().unwrap();
		let error = PoolFs::mount_sync(raw.clone(), PoolConfig::strict(0, 8)).err().unwrap();
		assert!(
			matches!(error, Error::Io(ref e) if e.kind() == io::ErrorKind::PermissionDenied),
			"{error:?}"
		);
		assert_eq!(raw.open_handles(), 1, "only the one that the test holds");
		assert_eq!(raw.leaked(), 0);
		held.close().unwrap();
		PoolFs::mount_sync(raw, PoolConfig::strict(0, 8)).expect("the lock was released");
	}

	#[tokio::test]
	async fn a_mount_waits_for_the_lock_and_then_takes_it() {
		let raw = MemRaw::new();
		let lock = poll_ready(raw.open_lock()).unwrap().unwrap();
		let cfg = PoolConfig {
			lock_deadline: Duration::from_secs(5),
			lock_poll: Duration::from_millis(10),
			..PoolConfig::memory()
		};
		let mounting = {
			let raw = raw.clone();
			tokio::spawn(async move { PoolFs::mount(raw, cfg).await.map(|_| ()) })
		};
		tokio::time::sleep(Duration::from_millis(100)).await;
		assert!(!mounting.is_finished(), "it is polling");
		lock.close().unwrap();
		tokio::time::timeout(Duration::from_secs(10), mounting)
			.await
			.expect("it mounts once the lock is free")
			.unwrap()
			.expect("mounted");
	}

	#[tokio::test]
	async fn a_mount_gives_up_when_the_lock_is_not_released() {
		let raw = MemRaw::new();
		let _lock = poll_ready(raw.open_lock()).unwrap().unwrap();
		let cfg = PoolConfig {
			lock_deadline: Duration::from_millis(150),
			lock_poll: Duration::from_millis(10),
			..PoolConfig::memory()
		};
		let started = rt::Instant::now();
		let error = PoolFs::mount(raw.clone(), cfg).await.err().unwrap();
		assert!(started.elapsed() >= Duration::from_millis(150), "it kept trying");
		match error {
			Error::Io(e) => {
				assert_eq!(e.kind(), io::ErrorKind::PermissionDenied);
				assert!(e.to_string().contains("another tab or worker"), "{e}");
			}
			other => panic!("{other:?}"),
		}
		// Without a deadline it is one try.
		let error = PoolFs::mount_sync(raw.clone(), PoolConfig::memory()).err().unwrap();
		assert!(matches!(error, Error::Io(ref e) if e.kind() == io::ErrorKind::PermissionDenied));
	}

	// ---- mount -------------------------------------------------------------------------------

	/// A directory in the state a crash and a bad day can leave it in.
	fn messy() -> MemRaw {
		let raw = MemRaw::new();
		let named = |seq, path: &str| Some(Header::named(seq, path));
		// The winner of "/d/a", and the older claimant that it beats.
		raw.put("p00000000", &slot_bytes(named(10, "/d/a"), Some(Header::free(4)), b"A10"));
		raw.put("p00000001", &slot_bytes(named(5, "/d/a"), None, b"old a"));
		// Free, with a payload that was never cleaned.
		raw.put("p00000002", &slot_bytes(Some(Header::free(3)), None, b"dead payload"));
		// Never given a header: empty, and shorter than a header.
		raw.put("p00000003", b"");
		raw.put("p00000004", &[7u8; 100]);
		// No valid header and a payload: an orphan.
		let mut orphan = slot_bytes(None, None, b"precious");
		orphan[10] = 0xAB;
		raw.put("p00000005", &orphan);
		// Two pages of one slot: the newer one counts, whichever page it is in.
		raw.put("p00000006", &slot_bytes(named(2, "/d/b"), named(7, "/d/b"), b"B"));
		// A header that was torn, over a free one.
		let mut torn = slot_bytes(named(9, "/e/torn"), Some(Header::free(1)), b"");
		torn[2000] ^= 0xFF;
		raw.put("p00000007", &torn);
		// Headers of zeros, and nothing after them.
		raw.put("p00000008", &vec![0u8; HEADER_LEN as usize]);
		// Names that are not slots.
		raw.put("junk", b"not a slot");
		raw.put("p0000000", b"too short");
		raw
	}

	#[test]
	fn a_mount_settles_what_it_finds() {
		let raw = messy();
		let orphan_before = raw.image("p00000005").unwrap();
		let (fs, report) = PoolFs::mount_sync(raw.clone(), PoolConfig::strict(0, 64)).unwrap();
		assert_eq!(report.files, 2);
		assert_eq!(report.losers_cleaned, 1, "the older claim of /d/a");
		assert_eq!(report.orphans_kept, vec!["p00000005".to_string()]);
		assert_eq!(report.blank_initialised, 3, "an empty slot, a short one and one of zeros");
		assert_eq!(report.cut_back, 1);
		assert_eq!(report.max_seq, 10);
		// Free: the loser, the dead one, the three blank ones, and the one with a torn header.
		assert_eq!(report.spares, 6);
		assert_eq!(
			logical(&fs),
			BTreeMap::from([
				("/d/a".to_string(), b"A10".to_vec()),
				("/d/b".to_string(), b"B".to_vec())
			])
		);
		// The orphan is as it was, and is never used: use up every free slot.
		assert_eq!(raw.image("p00000005").unwrap(), orphan_before);
		assert_eq!(fs.stats().orphans, 1);
		for i in 0..6 {
			fs.create_new(p(&format!("/d/n{i}")), CreateOpts::default()).unwrap();
		}
		assert!(
			fs.create_new(p("/d/n6"), CreateOpts::default()).is_err(),
			"the orphan is not free"
		);
		assert_eq!(raw.image("p00000005").unwrap(), orphan_before);
		// The loser and the dead one are clean, and the torn one is free.
		assert_eq!(raw.image("p00000001").unwrap().len() as u64, HEADER_LEN);
		assert_eq!(raw.image("p00000002").unwrap().len() as u64, HEADER_LEN);
		// Sequence numbers start above everything that was there.
		fs.replace_atomic(p("/d/M"), b"m").ok();
		let headers = headers_of(&raw, "p00000000");
		assert_eq!(
			headers.current.unwrap().1,
			Header::named(10, "/d/a"),
			"the winner was not written"
		);
		assert!(fs.shared.seq.load(Ordering::Acquire) > 10);
		assert!(fs.shared.next_phys.load(Ordering::Acquire) >= 9);
	}

	#[test]
	fn the_higher_sequence_wins_a_path_whichever_slot_it_is_in() {
		for (low_slot, high_slot) in [("p00000000", "p00000001"), ("p00000001", "p00000000")] {
			let raw = MemRaw::new();
			raw.put(low_slot, &slot_bytes(Some(Header::named(3, "/d/f")), None, b"low"));
			raw.put(high_slot, &slot_bytes(None, Some(Header::named(8, "/d/f")), b"high"));
			let (fs, report) = PoolFs::mount_sync(raw.clone(), PoolConfig::strict(0, 8)).unwrap();
			assert_eq!(read(&fs, "/d/f").unwrap(), b"high", "{low_slot} {high_slot}");
			assert_eq!(report.losers_cleaned, 1);
			// The loser is cut and free, so it cannot come back when the winner goes.
			fs.remove_file(p("/d/f")).unwrap();
			fs.pool().unwrap().close().unwrap();
			let (fs, _) = PoolFs::mount_sync(raw.clone(), PoolConfig::strict(0, 8)).unwrap();
			assert!(!fs.exists(p("/d/f")), "the older claim did not come back");
		}
	}

	#[test]
	fn a_newer_header_of_another_format_is_not_read_and_its_slot_is_kept() {
		let raw = MemRaw::new();
		let mut bytes = slot_bytes(Some(Header::named(4, "/d/f")), None, b"payload");
		bytes[8] = 2; // a format from the future
		raw.put("p00000000", &bytes);
		let (fs, report) = PoolFs::mount_sync(raw.clone(), PoolConfig::strict(0, 8)).unwrap();
		assert_eq!(report.orphans_kept, vec!["p00000000".to_string()]);
		assert!(!fs.exists(p("/d/f")));
		assert_eq!(raw.image("p00000000").unwrap(), bytes);
	}

	#[test]
	fn a_slot_that_will_not_open_fails_the_mount_with_its_error() {
		let raw = MemRaw::new();
		raw.put("p00000000", &slot_bytes(Some(Header::free(1)), None, b""));
		raw.max_open(Some(1)); // the lock takes the one
		let error = PoolFs::mount_sync(raw.clone(), PoolConfig::strict(0, 8)).err().unwrap();
		assert!(matches!(error, Error::Io(_)));
		assert_eq!(raw.open_handles(), 0, "nothing is left open");
	}

	/// The state that a mount leads to, and what it leaves on disk, as far as the mount decides it.
	fn settled(raw: &MemRaw) -> (BTreeMap<String, Vec<u8>>, Vec<String>, Vec<u8>) {
		let (fs, report) = PoolFs::mount_sync(raw.clone(), PoolConfig::strict(0, 64))
			.expect("a mount after a crash is not refused");
		assert_sound(raw, "after a mount");
		(logical(&fs), report.orphans_kept, raw.image("p00000005").unwrap())
	}

	#[test]
	fn a_crash_inside_a_mount_leads_to_the_same_mount_after_it() {
		let baseline = settled(&messy());
		// How many operations a clean mount makes.
		let probe = messy();
		let before = probe.ops();
		let _ = PoolFs::mount_sync(probe.clone(), PoolConfig::strict(0, 64)).unwrap();
		let total = probe.ops() - before;
		assert!(total > 10, "the mount of a messy directory does work: {total}");

		let mut checked = 0;
		for n in 0..=total {
			let raw = messy();
			raw.die_after(n);
			let outcome = PoolFs::mount_sync(raw.clone(), PoolConfig::strict(0, 64));
			let mut modes = vec![CrashMode::LoseUnsynced, CrashMode::ZeroTail];
			let pending = raw.unflushed_ops();
			modes.extend((0..=pending).map(CrashMode::Prefix));
			for k in 0..pending {
				for bytes in [1usize, 17, 4095] {
					modes.push(CrashMode::Tear(k, bytes));
				}
			}
			modes.extend((0..6).map(CrashMode::Reorder));
			drop(outcome);
			for mode in modes {
				let image = raw.crash(mode);
				let again = settled(&image);
				assert_eq!(again, baseline, "crash after {n} operations of the mount, {mode:?}");
				// And once more: a mount of a mounted image changes nothing.
				assert_eq!(settled(&image), baseline);
				checked += 1;
			}
		}
		assert!(checked > 200, "{checked}");
	}

	// ---- exhaustive crash points -------------------------------------------------------------

	type Check<'a> = &'a dyn Fn(&Arc<Mem>, &[&'static str], &str);

	/// A scenario, run once for every point at which the backend can be killed, and the state it is
	/// left in, crashed in every mode, checked.
	struct Scenario<'a> {
		prepare: &'a dyn Fn() -> (Arc<Mem>, MemRaw),
		run: &'a dyn Fn(&Arc<Mem>) -> Vec<&'static str>,
		check: Check<'a>,
	}

	fn crash_modes(raw: &MemRaw) -> Vec<CrashMode> {
		let pending = raw.unflushed_ops();
		let mut modes = vec![CrashMode::LoseUnsynced, CrashMode::ZeroTail];
		modes.extend((0..=pending).map(CrashMode::Prefix));
		for k in 0..pending {
			for bytes in [1usize, 17, 100, 2048, 4095] {
				modes.push(CrashMode::Tear(k, bytes));
			}
		}
		modes.extend((0..8).map(CrashMode::Reorder));
		modes
	}

	/// Returns how many crash images were checked.
	fn enumerate(scenario: &Scenario<'_>) -> usize {
		let (fs, raw) = (scenario.prepare)();
		let before = raw.ops();
		let _ = (scenario.run)(&fs);
		let total = raw.ops() - before;
		drop(fs);
		let mut checked = 0;
		for n in 0..=total {
			let (fs, raw) = (scenario.prepare)();
			raw.die_after(n);
			let acks = (scenario.run)(&fs);
			for mode in crash_modes(&raw) {
				let image = raw.crash(mode);
				let (mounted_fs, _) = PoolFs::mount_sync(image.clone(), PoolConfig::memory())
					.unwrap_or_else(|e| panic!("n={n} {mode:?}: a mount is never refused: {e}"));
				let context =
					format!("killed after {n} of {total} operations, {mode:?}, acked {acks:?}");
				assert_sound(&image, &context);
				// The directories are made again, as the engine does at every open.
				mounted_fs.create_dir_all(p("/d")).unwrap();
				(scenario.check)(&mounted_fs, &acks, &context);
				checked += 1;
			}
		}
		checked
	}

	fn base_pool() -> (Arc<Mem>, MemRaw) {
		let (fs, raw) = memory_pool();
		fs.create_dir_all(p("/d")).unwrap();
		(fs, raw)
	}

	#[test]
	fn every_crash_point_of_a_create_leaves_a_pool_that_mounts_and_keeps_what_was_synced() {
		let checked = enumerate(&Scenario {
			prepare: &|| {
				let (fs, raw) = base_pool();
				put(&fs, "/d/pre", b"pre");
				(fs, raw)
			},
			run: &|fs| {
				let mut acks = Vec::new();
				let Ok(a) = fs.create_new(p("/d/a"), CreateOpts::default()) else {
					return acks;
				};
				acks.push("created a");
				if a.append(b"payload").is_err() || a.sync().is_err() {
					return acks;
				}
				acks.push("synced a");
				if fs.sync_dir(p("/d")).is_err() {
					return acks;
				}
				let Ok(b) = fs.create_new(p("/d/b"), CreateOpts::default()) else {
					return acks;
				};
				if b.append(b"B").is_err() || fs.sync_dir(p("/d")).is_err() {
					return acks;
				}
				acks.push("b named durably");
				acks
			},
			check: &|fs, acks, context| {
				assert_eq!(read(fs, "/d/pre").as_deref(), Some(&b"pre"[..]), "{context}");
				if acks.contains(&"synced a") {
					assert_eq!(read(fs, "/d/a").as_deref(), Some(&b"payload"[..]), "{context}");
				}
				if acks.contains(&"b named durably") {
					assert_eq!(read(fs, "/d/b").as_deref(), Some(&b"B"[..]), "{context}");
				}
			},
		});
		assert!(checked > 200, "{checked}");
	}

	#[test]
	fn every_crash_point_of_a_rename_is_the_old_names_or_the_new_ones() {
		let checked = enumerate(&Scenario {
			prepare: &|| {
				let (fs, raw) = base_pool();
				put(&fs, "/d/a", b"A");
				put(&fs, "/d/b", b"B");
				(fs, raw)
			},
			run: &|fs| {
				let mut acks = Vec::new();
				if fs.rename(p("/d/a"), p("/d/b")).is_ok() {
					acks.push("renamed");
				}
				acks
			},
			check: &|fs, acks, context| {
				let (a, b) = (read(fs, "/d/a"), read(fs, "/d/b"));
				let before = a.as_deref() == Some(&b"A"[..]) && b.as_deref() == Some(&b"B"[..]);
				let after = a.is_none() && b.as_deref() == Some(&b"A"[..]);
				assert!(before || after, "{context}: a={a:?} b={b:?}");
				if acks.contains(&"renamed") {
					assert!(after, "{context}: a rename that returned is durable");
				}
				let listed = fs.read_dir(p("/d")).unwrap().len();
				assert_eq!(
					listed,
					if before {
						2
					} else {
						1
					},
					"{context}"
				);
			},
		});
		assert!(checked > 40, "{checked}");
	}

	#[test]
	fn every_crash_point_of_a_replace_leaves_exactly_the_old_content_or_exactly_the_new() {
		let old = b"the old manifest".to_vec();
		let new = b"the NEW manifest, which is longer than the old one".to_vec();
		let checked = enumerate(&Scenario {
			prepare: &|| {
				let (fs, raw) = base_pool();
				// Two generations, so that the next replacement finds a dead slot to clean.
				fs.replace_atomic(p("/d/MANIFEST"), b"generation zero").unwrap();
				fs.replace_atomic(p("/d/MANIFEST"), b"the old manifest").unwrap();
				(fs, raw)
			},
			run: &|fs| {
				let mut acks = Vec::new();
				if fs
					.replace_atomic(
						p("/d/MANIFEST"),
						b"the NEW manifest, which is longer than the old one",
					)
					.is_ok()
				{
					acks.push("replaced");
				}
				acks
			},
			check: &|fs, acks, context| {
				let content = read(fs, "/d/MANIFEST")
					.unwrap_or_else(|| panic!("{context}: the manifest is gone"));
				assert!(
					content == old || content == new,
					"{context}: {:?}",
					String::from_utf8_lossy(&content)
				);
				if acks.contains(&"replaced") {
					assert_eq!(content, new, "{context}: a replacement that returned is durable");
				}
				let names = fs.read_dir(p("/d")).unwrap();
				assert_eq!(names.len(), 1, "{context}: nothing else is left");
			},
		});
		assert!(checked > 100, "{checked}");
	}

	#[test]
	fn every_crash_point_of_a_create_truncate_keeps_the_old_file_or_an_empty_one() {
		let checked = enumerate(&Scenario {
			prepare: &|| {
				let (fs, raw) = base_pool();
				put(&fs, "/d/a", b"old");
				(fs, raw)
			},
			run: &|fs| {
				let mut acks = Vec::new();
				let Ok(file) = fs.create_truncate(p("/d/a"), CreateOpts::default()) else {
					return acks;
				};
				acks.push("truncated");
				let _ = file.append(b"new");
				acks
			},
			check: &|fs, acks, context| {
				let content =
					read(fs, "/d/a").unwrap_or_else(|| panic!("{context}: the file is gone"));
				if context.contains("LoseUnsynced") {
					let expected: &[u8] = if acks.contains(&"truncated") {
						b""
					} else {
						b"old"
					};
					assert_eq!(content, expected, "{context}");
				}
				assert_eq!(fs.read_dir(p("/d")).unwrap().len(), 1, "{context}");
			},
		});
		assert!(checked > 50, "{checked}");
	}

	#[test]
	fn every_crash_point_of_a_clean_leaves_the_file_gone_or_resurrected_whole_or_empty() {
		let stale = vec![0xEE; 300];
		let checked = enumerate(&Scenario {
			prepare: &|| {
				let (fs, raw) = base_pool();
				put(&fs, "/d/a", &[0xEE; 300]);
				put(&fs, "/d/keep", b"keep");
				fs.remove_file(p("/d/a")).unwrap();
				(fs, raw)
			},
			run: &|fs| {
				let mut acks = Vec::new();
				if fs.maintain(8) == 1 {
					acks.push("cleaned");
				}
				acks
			},
			check: &|fs, acks, context| {
				assert_eq!(read(fs, "/d/keep").as_deref(), Some(&b"keep"[..]), "{context}");
				match read(fs, "/d/a") {
					None => {}
					Some(content) => {
						assert!(
							content.is_empty() || content == vec![0xEE; 300],
							"{context}: a resurrected file is whole or empty, not a mixture"
						);
						assert!(
							!acks.contains(&"cleaned"),
							"{context}: a clean that returned is durable"
						);
					}
				}
			},
		});
		assert!(checked > 100, "{checked} (stale {})", stale.len());
	}

	#[test]
	fn every_crash_point_of_a_reuse_shows_nothing_of_the_slot_it_was_before() {
		let checked = enumerate(&Scenario {
			prepare: &|| {
				let (fs, raw) = base_pool();
				put(&fs, "/d/a", &[0xEE; 5000]);
				fs.remove_file(p("/d/a")).unwrap();
				(fs, raw)
			},
			run: &|fs| {
				let mut acks = Vec::new();
				let Ok(b) = fs.create_new(p("/d/b"), CreateOpts::default()) else {
					return acks;
				};
				if b.append(b"new bytes").is_err() || b.sync().is_err() {
					return acks;
				}
				acks.push("b synced");
				acks
			},
			check: &|fs, acks, context| {
				if let Some(content) = read(fs, "/d/b") {
					assert!(
						!content.contains(&0xEE),
						"{context}: b shows the old file: {content:?}"
					);
				}
				if acks.contains(&"b synced") {
					assert_eq!(read(fs, "/d/b").as_deref(), Some(&b"new bytes"[..]), "{context}");
				}
			},
		});
		assert!(checked > 100, "{checked}");
	}

	#[test]
	fn the_header_of_a_slot_survives_every_tear_of_its_own_write() {
		// One slot, one rename, and the header write is torn at every byte: the file is the old
		// name or the new one, and the data is the same.
		for torn in (0..=PAGE_LEN).step_by(37).chain([PAGE_LEN - 1, PAGE_LEN]) {
			let (fs, raw) = base_pool();
			put(&fs, "/d/a", b"AAAA");
			// Unflushed, so that the crash decides.
			let slot = Arc::clone(&fs.shared.ns.lock().files["/d/a"]);
			fs.shared.write_header(&slot, HeaderState::Named("/d/z".to_string())).unwrap();
			let image = raw.crash(CrashMode::Tear(0, torn));
			let (after, _) = PoolFs::mount_sync(image, PoolConfig::memory()).unwrap();
			let names: Vec<_> = logical(&after).keys().cloned().collect();
			assert!(
				names == vec!["/d/a".to_string()] || names == vec!["/d/z".to_string()],
				"torn at {torn}: {names:?}"
			);
			assert_eq!(logical(&after).values().next().unwrap(), b"AAAA");
		}
	}

	#[test]
	fn concurrent_header_writes_of_one_slot_leave_the_newest_page_counting() {
		let (fs, raw) = memory_pool();
		fs.create_dir_all(p("/d")).unwrap();
		put(&fs, "/d/a", b"A");
		let slot = Arc::clone(&fs.shared.ns.lock().files["/d/a"]);
		let threads: Vec<_> = (0..4)
			.map(|t| {
				let fs = Arc::clone(&fs);
				let slot = Arc::clone(&slot);
				std::thread::spawn(move || {
					for i in 0..200 {
						fs.shared
							.write_header(&slot, HeaderState::Named(format!("/d/t{t}_{i}")))
							.unwrap();
					}
				})
			})
			.collect();
		for thread in threads {
			thread.join().unwrap();
		}
		let (in_memory_seq, in_memory_path) = {
			let st = slot.st.lock();
			(st.seq, st.path.clone())
		};
		let on_disk = headers_of(&raw, &slot.name).current.unwrap().1;
		assert_eq!(on_disk.seq, in_memory_seq, "the page that counts is the one memory says");
		assert_eq!(on_disk.state, HeaderState::Named(in_memory_path.unwrap()));
	}

	// ---- the hooks and the file system that is public ---------------------------------------

	#[test]
	fn the_in_memory_file_system_needs_no_runtime() {
		assert!(tokio::runtime::Handle::try_current().is_err(), "this test runs outside a runtime");
		let fs = FileSystem::memory();
		assert_eq!(fs.0.kind(), FsKind::Pool);
		fs.0.check_available().unwrap();
		fs.0.create_dir_all(p("/db/wal")).unwrap();
		let file = fs.0.create_new(p("/db/wal/1.wal"), CreateOpts::default()).unwrap();
		file.append(b"x").unwrap();
		file.sync().unwrap();
		fs.0.sync_dir(p("/db/wal")).unwrap();
		fs.0.replace_atomic(p("/db/MANIFEST"), b"m").unwrap();
		assert!(fs.0.ensure_spares(50).is_none());
		let lock = fs.0.lock_file(p("/db"));
		drop(lock);
		for _ in 0..200 {
			let name = format!("/db/wal/{}", rand::random::<u64>());
			fs.0.create_new(p(&name), CreateOpts::default()).unwrap().append(b"y").unwrap();
		}
		assert_eq!(fs.0.read_dir(p("/db/wal")).unwrap().len(), 201);
	}

	#[test]
	fn the_hooks_of_the_tests_see_and_change_what_they_say() {
		let fs = FileSystem::memory();
		fs.0.create_dir_all(p("db/wal")).unwrap();
		let log =
			fs.0.create_new(p("db/wal/00000000000000000001.wal"), CreateOpts::default()).unwrap();
		log.append(&[1u8; 100]).unwrap();
		log.sync().unwrap();
		fs.0.sync_dir(p("db/wal")).unwrap();
		assert_eq!(fs.list_for_test("db/wal"), vec!["00000000000000000001.wal".to_string()]);
		assert!(fs.list_for_test("db/nothing").is_empty());
		assert_eq!(fs.file_len_for_test("db/wal/00000000000000000001.wal"), Some(100));
		assert_eq!(fs.file_len_for_test("db/wal/none"), None);
		assert_eq!(fs.mount_report().unwrap().files, 0, "the report is of the mount");

		// A cut without a flush: what a crash in the middle of a write leaves.
		log.append(&[2u8; 50]).unwrap();
		fs.raw_truncate_for_test("db/wal/00000000000000000001.wal", 103).unwrap();
		assert_eq!(log.len().unwrap(), 103);
		assert!(fs.raw_truncate_for_test("db/none", 1).is_err());

		// A crash keeps what was flushed, and the cut, which was not, is lost with the rest.
		let crashed = fs.simulate_crash(CrashMode::LoseUnsynced).unwrap();
		assert_eq!(
			crashed.file_len_for_test("db/wal/00000000000000000001.wal"),
			Some(100),
			"the directory is the ancestor of a file that is there"
		);
		let report = crashed.mount_report().unwrap();
		assert_eq!(report.files, 1);
		// The original is not touched by it.
		assert_eq!(log.len().unwrap(), 103);

		// The std file system has none of these.
		#[cfg(not(target_arch = "wasm32"))]
		{
			let std = FileSystem::std();
			assert!(std.simulate_crash(CrashMode::LoseUnsynced).is_err());
			assert!(std.raw_truncate_for_test("x", 1).is_err());
			assert!(std.mount_report().is_none());
			std.abandon();
		}
	}

	#[tokio::test]
	async fn closing_and_abandoning_through_the_public_type() {
		let fs = FileSystem::memory();
		fs.0.create_dir_all(p("db")).unwrap();
		let file = fs.0.create_new(p("db/f"), CreateOpts::default()).unwrap();
		file.append(b"abc").unwrap();
		fs.close().await.unwrap();
		assert_eq!(file.append(b"x").unwrap_err().kind(), io::ErrorKind::NotConnected);
		fs.close().await.expect("closing twice is not an error");
		#[cfg(not(target_arch = "wasm32"))]
		FileSystem::std().close().await.unwrap();

		let fs = FileSystem::memory();
		fs.0.create_dir_all(p("db")).unwrap();
		let file = fs.0.create_new(p("db/f"), CreateOpts::default()).unwrap();
		file.append(b"abc").unwrap();
		let crashed = fs.simulate_crash(CrashMode::Prefix(usize::MAX)).unwrap();
		fs.abandon();
		assert!(file.append(b"x").is_err());
		assert!(fs.0.check_available().is_err());
		// The crash image was made before the abandon, and is its own file system.
		crashed.0.create_dir_all(p("db")).unwrap();
		assert_eq!(crashed.file_len_for_test("db/f"), Some(3));
	}

	#[test]
	fn a_lock_is_one_per_directory_across_the_trees_of_one_pool() {
		let (fs, _) = memory_pool();
		let mut first = fs.lock_file(p("/db"));
		first.acquire().unwrap();
		let mut second = fs.lock_file(p("/db/./"));
		assert!(second.acquire().is_err());
		drop(first);
		second.acquire().unwrap();
	}

	// ---- the sizes that a disk gives ---------------------------------------------------------

	#[test]
	fn lengths_that_are_too_large_are_errors_and_not_panics() {
		let (fs, _) = memory_pool();
		fs.create_dir_all(p("/d")).unwrap();
		let file = fs.create_new(p("/d/f"), CreateOpts::default()).unwrap();
		file.append(b"abc").unwrap();
		assert_eq!(file.set_len(u64::MAX).unwrap_err().kind(), io::ErrorKind::InvalidInput);
		assert_eq!(
			file.set_len(u64::MAX - HEADER_LEN + 1).unwrap_err().kind(),
			io::ErrorKind::InvalidInput
		);
		assert_eq!(file.set_len(1 << 40).unwrap_err().kind(), io::ErrorKind::StorageFull);
		assert_eq!(file.len().unwrap(), 3, "a refused cut changes nothing");
		let mut buf = [0u8; 4];
		assert_eq!(file.read_at(u64::MAX, &mut buf).unwrap(), 0);
		assert_eq!(file.read_at(u64::MAX - 2, &mut buf).unwrap(), 0);
		assert_eq!(file.read_at(2, &mut buf).unwrap(), 1);
		assert_eq!(read(&fs, "/d/f").unwrap(), b"abc");
	}

	#[test]
	fn a_quota_surfaces_at_the_write_and_leaves_the_length_true() {
		let (fs, raw) = memory_pool();
		fs.create_dir_all(p("/d")).unwrap();
		let file = fs.create_new(p("/d/f"), CreateOpts::default()).unwrap();
		file.append(&[1u8; 10]).unwrap();
		raw.quota(Some(raw.image("p00000000").unwrap().len() as u64 + 20));
		file.append(&[2u8; 5]).unwrap();
		let error = file.append(&[3u8; 100]).unwrap_err();
		assert_eq!(error.kind(), io::ErrorKind::StorageFull);
		assert_eq!(file.len().unwrap(), 15, "nothing of the refused write is counted");
		raw.quota(None);
		file.append(b"more").unwrap();
		assert_eq!(file.len().unwrap(), 19);
	}

	#[test]
	fn a_write_that_is_cut_short_is_continued_and_one_that_makes_no_progress_fails() {
		let (fs, raw) = memory_pool();
		fs.create_dir_all(p("/d")).unwrap();
		let file = fs.create_new(p("/d/f"), CreateOpts::default()).unwrap();
		raw.short_write(Some(3));
		file.append(b"0123456789").unwrap();
		assert_eq!(read_all(&file), b"0123456789");
		raw.short_write(Some(0));
		let error = file.append(b"x").unwrap_err();
		assert_eq!(error.kind(), io::ErrorKind::WriteZero);
		assert_eq!(file.len().unwrap(), 10);
		raw.short_write(None);
		// The header write is looped over too.
		raw.short_write(Some(100));
		fs.create_new(p("/d/g"), CreateOpts::default()).unwrap();
		raw.short_write(None);
		assert!(fs.exists(p("/d/g")));
	}

	#[test]
	fn a_failed_header_write_creates_nothing_and_gives_the_slot_back() {
		let (fs, raw) = strict_pool(2, 2);
		fs.create_dir_all(p("/d")).unwrap();
		raw.fail_write_after(0);
		assert!(fs.create_new(p("/d/a"), CreateOpts::default()).is_err());
		assert!(!fs.exists(p("/d/a")));
		assert_eq!(fs.stats().dirty, 1, "the slot is dead and will be cleaned");
		fs.create_new(p("/d/a"), CreateOpts::default()).expect("the same slot, cleaned, serves");
		fs.create_new(p("/d/b"), CreateOpts::default()).expect("and the other");
		assert_eq!(fs.stats().total, 2);
	}

	#[test]
	fn a_manifest_that_fails_before_its_commit_leaves_the_old_one_and_no_trace() {
		let (fs, raw) = strict_pool(3, 3);
		fs.create_dir_all(p("/d")).unwrap();
		fs.replace_atomic(p("/d/M"), b"old").unwrap();
		// The content is the first write and its flush the first flush: a failure of either is an
		// I/O error, and the old content stays.
		raw.fail_write_after(0);
		assert!(matches!(fs.replace_atomic(p("/d/M"), b"new content"), Err(Error::Io(_))));
		assert_eq!(read(&fs, "/d/M").unwrap(), b"old");
		raw.clear_faults();
		raw.fail_flush_after(0);
		assert!(matches!(fs.replace_atomic(p("/d/M"), b"x"), Err(Error::Io(_))), "the first flush");
		assert_eq!(read(&fs, "/d/M").unwrap(), b"old");
		raw.clear_faults();
		// Both slots that were given back are usable again.
		fs.replace_atomic(p("/d/M"), b"new").unwrap();
		assert_eq!(read(&fs, "/d/M").unwrap(), b"new");
		assert_eq!(fs.stats().poisoned, 0);
		assert_eq!(fs.stats().total, 3);
	}

	#[test]
	fn a_manifest_whose_commit_fails_is_uncertain_and_its_slot_is_never_reused() {
		for fault in 0..2 {
			let (fs, raw) = strict_pool(4, 4);
			fs.create_dir_all(p("/d")).unwrap();
			fs.replace_atomic(p("/d/M"), b"old").unwrap();
			// The commit is the second write (the header) and the second flush.
			match fault {
				0 => raw.fail_write_after(1),
				_ => raw.fail_flush_after(1),
			}
			let error = fs.replace_atomic(p("/d/M"), b"new").unwrap_err();
			assert!(matches!(error, Error::ManifestWriteUncertain(_)), "{error:?}");
			assert_eq!(fs.stats().poisoned, 1);
			assert_eq!(read(&fs, "/d/M").unwrap(), b"old", "memory still has the old one");
			// A mount may find either: the slot is not given back to anything.
			let slot_count = fs.stats().total;
			let mut held = 0;
			while fs.create_new(p(&format!("/d/f{held}")), CreateOpts::default()).is_ok() {
				held += 1;
			}
			assert_eq!(
				held,
				slot_count - 2,
				"two slots are the live manifest and the poisoned one"
			);
			for i in 0..held {
				fs.remove_file(p(&format!("/d/f{i}"))).unwrap();
			}
			fs.maintain(usize::MAX);
			// The next replacement works, and a mount sees the highest sequence.
			raw.clear_faults();
			fs.replace_atomic(p("/d/M"), b"newer").unwrap();
			let image = raw.snapshot();
			let (after, _) = PoolFs::mount_sync(image, PoolConfig::memory()).unwrap();
			assert_eq!(read(&after, "/d/M").unwrap(), b"newer");
		}
	}

	// ---- handles and runtimes ----------------------------------------------------------------

	#[test]
	fn a_pool_holds_one_handle_per_slot_and_one_for_the_lock() {
		let (fs, raw) = strict_pool(10, 64);
		assert_eq!(raw.open_handles(), 11);
		fs.create_dir_all(p("/d")).unwrap();
		// Files, however many times they are opened and read, are views of the slots.
		let files: Vec<_> = (0..5).map(|i| put(&fs, &format!("/d/{i}"), b"x")).collect();
		for _ in 0..20 {
			for i in 0..5 {
				let view = fs.open_read(p(&format!("/d/{i}"))).unwrap();
				let _ = fs.open_append(p(&format!("/d/{i}")), CreateOpts::default()).unwrap();
				drop(view);
			}
		}
		fs.replace_atomic(p("/d/M"), b"a").unwrap();
		fs.replace_atomic(p("/d/M"), b"b").unwrap();
		assert_eq!(raw.open_handles(), 11, "no call made a handle");
		assert_eq!(raw.leaked(), 0);
		drop(files);
		fs.pool().unwrap().close().unwrap();
		assert_eq!(raw.open_handles(), 0);
	}

	#[test]
	fn a_pool_with_a_refill_task_still_works_where_there_is_no_runtime() {
		assert!(tokio::runtime::Handle::try_current().is_err());
		let raw = MemRaw::new();
		let cfg = PoolConfig {
			background_refill: true,
			low: 3,
			high: 6,
			initial: 2,
			max_files: 32,
			..PoolConfig::default()
		};
		let (fs, _) = PoolFs::mount_sync(raw, cfg).unwrap();
		assert!(!fs.background_alive(), "there is nothing to run a task on");
		fs.create_dir_all(p("/d")).unwrap();
		// A claim that leaves the pool low asks for a refill, and nobody is there to make it.
		fs.create_new(p("/d/a"), CreateOpts::default()).unwrap();
		assert!(!fs.background_alive());
		// A wait does the refill itself.
		poll_ready(fs.ensure_spares(6).unwrap()).unwrap().unwrap();
		assert!(fs.stats().clean >= 6);
	}

	#[test]
	fn a_pool_outlives_the_runtime_that_ran_its_refill_task() {
		let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		let cfg = PoolConfig {
			background_refill: true,
			low: 2,
			high: 8,
			initial: 2,
			max_files: 64,
			..PoolConfig::default()
		};
		let (fs, _) = runtime.block_on(async { PoolFs::mount(MemRaw::new(), cfg).await.unwrap() });
		assert!(fs.background_alive());
		drop(runtime);
		assert!(!fs.background_alive(), "the task went with its runtime, and says so");
		// So a wait does not wait for it for ever.
		poll_ready(fs.ensure_spares(20).unwrap()).expect("the wait completes").unwrap();
		assert!(fs.stats().clean >= 20);
		// And a runtime that is there later gets the task again.
		let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		runtime.block_on(async {
			fs.create_dir_all(p("/d")).unwrap();
			for i in 0..19 {
				fs.create_new(p(&format!("/d/{i}")), CreateOpts::default()).unwrap();
			}
			assert!(fs.background_alive(), "a claim that leaves the pool low starts the task");
		});
	}

	// ---- a model --------------------------------------------------------------------------------

	const PATHS: [&str; 4] = ["/m/d/a", "/m/d/b", "/m/d/c", "/m/e/x"];

	fn dir_of(i: usize) -> &'static str {
		if i < 3 {
			"/m/d"
		} else {
			"/m/e"
		}
	}

	#[derive(Clone, Debug)]
	enum Op {
		New(usize),
		AppendOpen(usize),
		Trunc(usize),
		OpenRead(usize),
		Append(usize, Vec<u8>),
		SetLen(usize, usize),
		Sync(usize),
		SyncDir(usize),
		Remove(usize),
		Rename(usize, usize),
		Replace(usize, Vec<u8>),
		Maintain,
		Drop(usize),
		Check(usize),
		Crash(u8, u64),
	}

	fn ops() -> impl Strategy<Value = Vec<Op>> {
		let bytes = |max| prop::collection::vec(any::<u8>(), 0..max);
		let op = prop_oneof![
			3 => (0..4usize).prop_map(Op::New),
			3 => (0..4usize).prop_map(Op::AppendOpen),
			2 => (0..4usize).prop_map(Op::Trunc),
			1 => (0..4usize).prop_map(Op::OpenRead),
			6 => (any::<usize>(), bytes(24)).prop_map(|(h, b)| Op::Append(h, b)),
			2 => (any::<usize>(), 0..48usize).prop_map(|(h, l)| Op::SetLen(h, l)),
			4 => any::<usize>().prop_map(Op::Sync),
			2 => (0..2usize).prop_map(Op::SyncDir),
			3 => (0..4usize).prop_map(Op::Remove),
			3 => (0..4usize, 0..4usize).prop_map(|(a, b)| Op::Rename(a, b)),
			3 => (0..4usize, bytes(30)).prop_map(|(i, b)| Op::Replace(i, b)),
			2 => Just(Op::Maintain),
			2 => any::<usize>().prop_map(Op::Drop),
			2 => (0..4usize).prop_map(Op::Check),
			2 => (any::<u8>(), any::<u64>()).prop_map(|(m, s)| Op::Crash(m, s)),
		];
		prop::collection::vec(op, 1..48)
	}

	type Content = Rc<RefCell<Vec<u8>>>;

	/// What the pool is supposed to be, and what a crash is allowed to do to it.
	#[derive(Default)]
	struct Model {
		files: BTreeMap<usize, Content>,
		handles: Vec<(Arc<dyn FsFile>, Content)>,
		/// Content that is durable at a path, exactly: what a crash that loses everything unsynced
		/// has to leave.
		must: BTreeMap<usize, Vec<u8>>,
		/// Every content that was ever made durable at a path since the last mount: a file that is
		/// there after a crash and is not in `must` is one of these, which a removal that was not
		/// made durable brings back.
		may: BTreeMap<usize, Vec<Vec<u8>>>,
		/// Names that were made and not flushed.
		name_dirty: BTreeSet<usize>,
	}

	impl Model {
		fn durable(&mut self, i: usize, bytes: Vec<u8>) {
			self.must.insert(i, bytes.clone());
			self.may.entry(i).or_default().push(bytes);
			self.name_dirty.remove(&i);
		}

		fn path_of(&self, content: &Content) -> Option<usize> {
			self.files.iter().find(|(_, c)| Rc::ptr_eq(c, content)).map(|(i, _)| *i)
		}
	}

	fn crash_mode(selector: u8, seed: u64) -> CrashMode {
		match selector % 5 {
			0 => CrashMode::LoseUnsynced,
			1 => CrashMode::Prefix((seed % 40) as usize),
			2 => CrashMode::Tear((seed % 40) as usize, ((seed / 40) % 300) as usize),
			3 => CrashMode::ZeroTail,
			_ => CrashMode::Reorder(seed),
		}
	}

	fn setup_dirs(fs: &Mem) {
		fs.create_dir_all(p("/m/d")).unwrap();
		fs.create_dir_all(p("/m/e")).unwrap();
	}

	fn check_all(fs: &Mem, model: &Model, context: &str) {
		for (i, path) in PATHS.iter().enumerate() {
			let expected = model.files.get(&i).map(|c| c.borrow().clone());
			assert_eq!(read(fs, path), expected, "{context}: {path}");
			assert_eq!(
				fs.entry(p(path)).unwrap(),
				expected.as_ref().map(|_| EntryKind::File),
				"{context}: {path}"
			);
		}
		for (dir, range) in [("/m/d", 0..3), ("/m/e", 3..4)] {
			let mut listed: Vec<String> = fs
				.read_dir(p(dir))
				.unwrap()
				.into_iter()
				.map(|e| e.name.to_string_lossy().into_owned())
				.collect();
			listed.sort();
			let mut expected: Vec<String> = range
				.filter(|i| model.files.contains_key(i))
				.map(|i| PATHS[i].rsplit('/').next().unwrap().to_string())
				.collect();
			expected.sort();
			assert_eq!(listed, expected, "{context}: {dir}");
		}
		for (handle, content) in &model.handles {
			assert_eq!(&read_all(handle), &*content.borrow(), "{context}: a handle");
			assert_eq!(handle.len().unwrap(), content.borrow().len() as u64);
		}
	}

	fn run_model(script: Vec<Op>) {
		let mut raw = MemRaw::new();
		let mut fs = mounted(&raw, PoolConfig::memory());
		setup_dirs(&fs);
		let mut m = Model::default();
		let new_content = |bytes: &[u8]| Rc::new(RefCell::new(bytes.to_vec()));

		for (step, op) in script.into_iter().enumerate() {
			let context = format!("step {step}: {op:?}");
			match op {
				Op::New(i) => {
					let result = fs.create_new(p(PATHS[i]), CreateOpts::default());
					if m.files.contains_key(&i) {
						assert_eq!(
							result.err().map(|e| e.kind()),
							Some(io::ErrorKind::AlreadyExists),
							"{context}"
						);
					} else {
						let content = new_content(b"");
						m.handles.push((result.expect(&context), Rc::clone(&content)));
						m.files.insert(i, content);
						m.name_dirty.insert(i);
					}
				}
				Op::AppendOpen(i) => {
					let handle =
						fs.open_append(p(PATHS[i]), CreateOpts::default()).expect(&context);
					let content = match m.files.get(&i) {
						Some(content) => Rc::clone(content),
						None => {
							let content = new_content(b"");
							m.files.insert(i, Rc::clone(&content));
							m.name_dirty.insert(i);
							content
						}
					};
					m.handles.push((handle, content));
				}
				Op::Trunc(i) => {
					let handle =
						fs.create_truncate(p(PATHS[i]), CreateOpts::default()).expect(&context);
					let content = new_content(b"");
					let existed = m.files.insert(i, Rc::clone(&content)).is_some();
					m.handles.push((handle, content));
					if existed {
						m.durable(i, Vec::new());
					} else {
						m.name_dirty.insert(i);
					}
				}
				Op::OpenRead(i) => {
					let result = fs.open_read(p(PATHS[i]));
					match m.files.get(&i) {
						Some(content) => {
							m.handles.push((result.expect(&context), Rc::clone(content)))
						}
						None => assert_eq!(
							result.err().map(|e| e.kind()),
							Some(io::ErrorKind::NotFound),
							"{context}"
						),
					}
				}
				Op::Append(h, bytes) => {
					if m.handles.is_empty() {
						continue;
					}
					let (handle, content) = &m.handles[h % m.handles.len()];
					handle.append(&bytes).expect(&context);
					content.borrow_mut().extend_from_slice(&bytes);
				}
				Op::SetLen(h, len) => {
					if m.handles.is_empty() {
						continue;
					}
					let (handle, content) = &m.handles[h % m.handles.len()];
					handle.set_len(len as u64).expect(&context);
					content.borrow_mut().resize(len, 0);
				}
				Op::Sync(h) => {
					if m.handles.is_empty() {
						continue;
					}
					let (handle, content) = m.handles[h % m.handles.len()].clone();
					handle.sync().expect(&context);
					if let Some(i) = m.path_of(&content) {
						let bytes = content.borrow().clone();
						m.durable(i, bytes);
					}
				}
				Op::SyncDir(d) => {
					let dir = if d == 0 {
						"/m/d"
					} else {
						"/m/e"
					};
					fs.sync_dir(p(dir)).expect(&context);
					let dirty: Vec<usize> =
						m.name_dirty.iter().copied().filter(|i| dir_of(*i) == dir).collect();
					for i in dirty {
						let bytes = m.files[&i].borrow().clone();
						m.durable(i, bytes);
					}
				}
				Op::Remove(i) => {
					let result = fs.remove_file(p(PATHS[i]));
					if m.files.remove(&i).is_some() {
						result.expect(&context);
						m.must.remove(&i);
						m.name_dirty.remove(&i);
					} else {
						assert_eq!(
							result.err().map(|e| e.kind()),
							Some(io::ErrorKind::NotFound),
							"{context}"
						);
					}
				}
				Op::Rename(a, b) => {
					let result = fs.rename(p(PATHS[a]), p(PATHS[b]));
					match m.files.get(&a).cloned() {
						None => assert_eq!(
							result.err().map(|e| e.kind()),
							Some(io::ErrorKind::NotFound),
							"{context}"
						),
						Some(content) => {
							result.expect(&context);
							if a != b {
								m.files.remove(&a);
								m.files.insert(b, Rc::clone(&content));
								m.must.remove(&a);
								m.name_dirty.remove(&a);
								let bytes = content.borrow().clone();
								m.durable(b, bytes);
							}
						}
					}
				}
				Op::Replace(i, bytes) => {
					fs.replace_atomic(p(PATHS[i]), &bytes).expect(&context);
					m.files.insert(i, new_content(&bytes));
					m.durable(i, bytes);
				}
				Op::Maintain => {
					fs.maintain(usize::MAX);
					assert_eq!(fs.stats().dirty, 0, "{context}");
				}
				Op::Drop(h) => {
					if !m.handles.is_empty() {
						let at = h % m.handles.len();
						m.handles.remove(at);
					}
				}
				Op::Check(_) => check_all(&fs, &m, &context),
				Op::Crash(selector, seed) => {
					let mode = crash_mode(selector, seed);
					let image = raw.crash(mode);
					m.handles.clear();
					let (after, _) = PoolFs::mount_sync(image.clone(), PoolConfig::memory())
						.unwrap_or_else(|e| panic!("{context} {mode:?}: mount refused: {e}"));
					assert_sound(&image, &context);
					setup_dirs(&after);
					let mut observed = BTreeMap::new();
					for (i, path) in PATHS.iter().enumerate() {
						if let Some(bytes) = read(&after, path) {
							// What is there is something that was made durable, when everything
							// unsynced is lost.
							if mode == CrashMode::LoseUnsynced {
								if let Some(must) = m.must.get(&i) {
									assert_eq!(
										&bytes, must,
										"{context}: {path} lost what was synced"
									);
								} else {
									assert!(
										m.may.get(&i).is_some_and(|may| may.contains(&bytes)),
										"{context}: {path} came back as something that was never durable: {bytes:?}"
									);
								}
							}
							observed.insert(i, bytes);
						}
					}
					for i in m.must.keys() {
						assert!(
							observed.contains_key(i),
							"{context} {mode:?}: {} is gone though it was synced",
							PATHS[*i]
						);
					}
					// The new state is what a crash that loses nothing more would leave.
					m = Model::default();
					for (i, bytes) in observed {
						m.files.insert(i, new_content(&bytes));
						m.must.insert(i, bytes.clone());
						m.may.insert(i, vec![bytes]);
					}
					raw = image;
					fs = after;
					check_all(&fs, &m, &context);
				}
			}
		}
		let context = "at the end";
		check_all(&fs, &m, context);
		assert_slots(&raw, context, false);
	}

	proptest! {
		#![proptest_config(ProptestConfig {
			cases: 400,
			max_shrink_iters: 4000,
			..ProptestConfig::default()
		})]

		#[test]
		fn the_pool_behaves_as_the_model_does_and_a_crash_keeps_what_was_synced(script in ops()) {
			run_model(script);
		}
	}

	#[test]
	fn the_model_runs_on_a_script_that_crashes_often() {
		// A fixed script, for the case where shrinking is not wanted: synced content survives a
		// crash, an unsynced file does not, and a removal that was not synced comes back.
		run_model(vec![
			Op::New(0),
			Op::Append(0, b"hello".to_vec()),
			Op::Sync(0),
			Op::Append(0, b" world".to_vec()),
			Op::New(1),
			Op::Append(1, b"lost".to_vec()),
			Op::Crash(0, 0),
			Op::Check(0),
			Op::Remove(0),
			Op::Crash(0, 0),
			Op::Check(0),
			Op::Replace(2, b"manifest".to_vec()),
			Op::Replace(2, b"manifest two".to_vec()),
			Op::Maintain,
			Op::Rename(2, 3),
			Op::Crash(1, 3),
			Op::Check(3),
		]);
	}
}
