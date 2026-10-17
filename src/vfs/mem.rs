//! `MemRaw`: the slot files of a pool, in memory, with a model of what a crash keeps.
//!
//! It is the backend under `FileSystem::memory()`, and under every test of the pool: the pool runs
//! the same code over it as it runs over a browser's origin private file system, and this is where
//! the crash analysis of the pool is tested, with the file system made to fail where a browser
//! cannot be.
//!
//! # What a crash keeps
//!
//! Every file has the bytes that a read sees (`current`) and the bytes that `flush` has made
//! durable (`durable`), and the writes and truncations since the last flush, each with a number
//! from one count for the whole backend, in order (`pending`). A crash image is a new backend
//! whose files are the durable bytes with some of the pending operations applied, as the
//! [`CrashMode`] says. Nothing that was flushed is ever lost, and a file that exists keeps existing
//! (see "What is not modelled" for what that leaves out).
//!
//! # What is not modelled
//!
//! The crash analysis of the pool is only as good as this model of a browser, which is the model of
//! the people who wrote the pool. These are the places where a browser may do something that this
//! backend never does, and where a test that passes over it says nothing:
//!
//! * **The loss of a slot file, or of its directory entry (E6).** A crash here never loses a file
//!   that was created, not even one that was never flushed, and never brings back one that was
//!   removed. A browser may keep the size of a file and lose its name, or the other way round. The
//!   pool does not depend on it, since it flushes the two header pages of a slot that it creates
//!   before it uses the slot, and a slot that a mount does not find is a slot that is lost and not
//!   one that is wrong: but that is an argument, and nothing here runs it. `MemRaw::remove` builds
//!   the state by hand, for a test that wants to look at one case.
//! * **A flush that is not a durability barrier.** The pool takes `flush` for an ordering barrier
//!   only: what was written before it is ordered before what was written after it, and nothing is
//!   promised about a power cut. Here a flush that returns makes every operation before it durable,
//!   at once and for ever, which is the strongest thing a flush could be. The modes that lose
//!   unflushed operations are what stands in for the weaker ones, and a flush that returns and is
//!   lost anyway (the platform did not write it to the medium) is not among them.
//! * **The order and the atomicity of what a crash keeps.** `Reorder` and `LoseTear` pick some
//!   subsets and some tears, in a model of how a cache writes blocks back. A real device may keep a
//!   write whole in two places and torn in a third, or tear below the size of a header page in a
//!   way that a prefix of the write does not describe.
//! * **Time.** Nothing is slow, and a handle that is not closed is only locked in a backend that is
//!   strict: a browser releases it at an unspecified time after the handle is collected.
//! * **Quota and handle limits** are single numbers that a test sets. A browser says
//!   `QuotaExceededError` from `write` and from `truncate`, and may say it later than a number
//!   does.
//!
//! # Faults
//!
//! Faults are set on a backend that a test holds a clone of. They fail the next write, flush or
//! truncation (once), cut a write short, fail a write after some of its bytes landed, refuse growth
//! past a quota with `StorageFull`, refuse an open past a number of handles, or kill the backend:
//! after `die_after(n)` operations every operation fails, and nothing that was going to happen
//! after that does, which is what a crash at that point looks like to the code that ran. An
//! exhaustive crash test runs a scenario once for each `n`, and applies every mode to what is left.
//!
//! A call can also be made to hang (`hang_after`, `hang_name`): its future is pending for ever, is
//! never woken, and does nothing, which is a request that a browser never answers. It is what a
//! test of a future that is dropped half way waits for, and since the backend has done nothing for
//! it, whatever such a test finds left open afterwards is a leak of the code under test.
//!
//! # Handles
//!
//! A handle must be closed. One that is dropped without a close is counted (`leaked`), and in a
//! strict backend it keeps its file locked, as a handle of a browser does until it is collected:
//! a pool that forgets a close fails the next open of that file in a test, and not in a browser.
//! Strict also makes a second open of one file an error, and makes every create and open complete
//! only after a pending poll, with the operation done when the future is first polled, so a future
//! that is dropped half way leaves a handle that nobody closes, exactly as a browser does.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use parking_lot::Mutex;

use super::pool::{Raw, RawFile, RawFut, LOCK_NAME};

/// What a crash does to the operations that were not flushed. See the module.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashMode {
	/// Only what was flushed survives.
	LoseUnsynced,
	/// The first `k` unflushed operations (writes and truncations of every file, in the order
	/// they were made) survive, and the rest are lost.
	Prefix(usize),
	/// The first `k` survive, and the next one, if it is a write, survives only in its first `n`
	/// bytes: a write that was torn.
	Tear(usize, usize),
	/// Every unflushed operation survives except the one at index `lost`, which is dropped, and
	/// the write at index `torn`, which survives in its first `n` bytes. A flush is the only
	/// barrier, so an earlier write may be lost while a later one is torn: no other mode says so.
	LoseTear {
		lost: usize,
		torn: usize,
		n: usize,
	},
	/// Every operation survives as to the length of the file, and none as to the bytes: the size
	/// of a file was kept and the blocks were not. The bytes that the file grew by read as
	/// zeros, and a write over bytes that were durable is lost, so the old ones stay (a
	/// browser zero-fills growth, and never writes zeros over what was there).
	ZeroTail,
	/// A seeded subset survives: later writes tend to, truncations tend not to. An operation that
	/// persists while one before it does not is a reordering.
	Reorder(u64),
}

/// How a backend behaves, besides what it is asked to do.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MemConfig {
	/// A second open of a file is an error, a handle that is dropped keeps its file locked, and
	/// the lock of the pool is exclusive too (it always is).
	pub(crate) strict: bool,
	/// Keep the unflushed operations, so that a crash can lose them. A backend that does not track
	/// them keeps one copy of every file and a crash loses nothing.
	pub(crate) track_unsynced: bool,
	/// How many times a create, open or list says "not yet" before it completes. A test can change
	/// it later with `MemRaw::set_pending_polls`.
	pub(crate) pending_polls: usize,
}

impl Default for MemConfig {
	fn default() -> Self {
		Self {
			strict: false,
			track_unsynced: true,
			pending_polls: 0,
		}
	}
}

impl MemConfig {
	/// The configuration of the strict tests.
	#[cfg_attr(not(test), allow(dead_code, reason = "the strict backend of the tests"))]
	pub(crate) fn strict() -> Self {
		Self {
			strict: true,
			track_unsynced: true,
			pending_polls: 1,
		}
	}
}

/// The largest file a backend holds. A file of a test is small, and an offset that is not is a
/// bug that must not take the machine's memory.
const MAX_FILE_LEN: u64 = 1 << 34;

#[derive(Clone, Debug)]
enum Op {
	Write {
		offset: u64,
		data: Vec<u8>,
	},
	SetLen(u64),
}

#[derive(Default)]
struct FileInner {
	current: Vec<u8>,
	durable: Vec<u8>,
	pending: Vec<(u64, Op)>,
}

struct MemFile {
	inner: Mutex<FileInner>,
	/// A handle holds the file: exclusive in a strict backend and for the lock of the pool.
	locked: AtomicBool,
}

impl MemFile {
	fn new(durable: Vec<u8>) -> Self {
		Self {
			inner: Mutex::new(FileInner {
				current: durable.clone(),
				durable,
				pending: Vec::new(),
			}),
			locked: AtomicBool::new(false),
		}
	}
}

#[derive(Default)]
struct Faults {
	die_after: Option<u64>,
	dead: bool,
	fail_write_after: Option<usize>,
	fail_flush_after: Option<usize>,
	fail_truncate_after: Option<usize>,
	short_write: Option<usize>,
	write_then_fail: Option<usize>,
	quota: Option<u64>,
	max_open: Option<usize>,
}

/// Which calls never complete. See `MemRaw::hang_after` and `MemRaw::hang_name`.
#[derive(Default)]
struct Hangs {
	/// The number of calls that still complete before every later one hangs.
	after: Option<usize>,
	/// The names whose calls hang.
	names: BTreeSet<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
	Write,
	SetLen,
	Flush,
	Create,
}

struct MemState {
	cfg: MemConfig,
	files: Mutex<BTreeMap<String, Arc<MemFile>>>,
	/// The number of the next mutating operation, over all files.
	op_seq: AtomicU64,
	/// The mutating operations that passed the gate.
	ops_done: AtomicU64,
	faults: Mutex<Faults>,
	/// The sum of the lengths of every file, for the quota.
	total_bytes: AtomicU64,
	pending_polls: AtomicUsize,
	hangs: Mutex<Hangs>,
	hung: AtomicUsize,
	writes: AtomicU64,
	flushes: AtomicU64,
	creates: AtomicU64,
	open_handles: AtomicUsize,
	leaked: AtomicUsize,
}

fn dead_error() -> io::Error {
	io::Error::new(
		io::ErrorKind::BrokenPipe,
		"the backend was killed: nothing happens after a crash",
	)
}

fn injected(what: &str) -> io::Error {
	io::Error::other(format!("injected failure of {what}"))
}

fn too_large() -> io::Error {
	io::Error::new(io::ErrorKind::StorageFull, "a file of the memory backend would be too large")
}

fn apply(buf: &mut Vec<u8>, op: &Op) -> io::Result<()> {
	match op {
		Op::Write {
			offset,
			data,
		} => {
			let end = offset.checked_add(data.len() as u64).ok_or_else(too_large)?;
			if end > MAX_FILE_LEN {
				return Err(too_large());
			}
			let start = usize::try_from(*offset).map_err(|_| too_large())?;
			let end = usize::try_from(end).map_err(|_| too_large())?;
			if end > buf.len() {
				buf.resize(end, 0);
			}
			buf[start..end].copy_from_slice(data);
		}
		Op::SetLen(len) => {
			if *len > MAX_FILE_LEN {
				return Err(too_large());
			}
			buf.resize(usize::try_from(*len).map_err(|_| too_large())?, 0);
		}
	}
	Ok(())
}

impl MemState {
	fn new(cfg: MemConfig) -> Self {
		Self {
			cfg,
			files: Mutex::new(BTreeMap::new()),
			op_seq: AtomicU64::new(0),
			ops_done: AtomicU64::new(0),
			faults: Mutex::new(Faults::default()),
			total_bytes: AtomicU64::new(0),
			pending_polls: AtomicUsize::new(cfg.pending_polls),
			hangs: Mutex::new(Hangs::default()),
			hung: AtomicUsize::new(0),
			writes: AtomicU64::new(0),
			flushes: AtomicU64::new(0),
			creates: AtomicU64::new(0),
			open_handles: AtomicUsize::new(0),
			leaked: AtomicUsize::new(0),
		}
	}

	/// The gate of every operation that changes something: the kill, and the faults that fire
	/// once.
	fn begin(&self, kind: Kind) -> io::Result<()> {
		let mut faults = self.faults.lock();
		if faults.dead {
			return Err(dead_error());
		}
		if let Some(left) = faults.die_after {
			if left == 0 {
				faults.dead = true;
				return Err(dead_error());
			}
			faults.die_after = Some(left - 1);
		}
		let (slot, what) = match kind {
			Kind::Write => (&mut faults.fail_write_after, "a write"),
			Kind::Flush => (&mut faults.fail_flush_after, "a flush"),
			Kind::SetLen => (&mut faults.fail_truncate_after, "a truncation"),
			Kind::Create => {
				self.ops_done.fetch_add(1, Ordering::SeqCst);
				return Ok(());
			}
		};
		match slot {
			Some(0) => {
				*slot = None;
				Err(injected(what))
			}
			Some(n) => {
				*n -= 1;
				self.ops_done.fetch_add(1, Ordering::SeqCst);
				Ok(())
			}
			None => {
				self.ops_done.fetch_add(1, Ordering::SeqCst);
				Ok(())
			}
		}
	}

	fn check_alive(&self) -> io::Result<()> {
		if self.faults.lock().dead {
			Err(dead_error())
		} else {
			Ok(())
		}
	}

	fn next_op(&self) -> u64 {
		self.op_seq.fetch_add(1, Ordering::SeqCst)
	}

	/// Opens a handle on `file`, under the rules of the configuration.
	fn handle(self: &Arc<Self>, file: &Arc<MemFile>, exclusive: bool) -> io::Result<MemRawFile> {
		let exclusive = exclusive || self.cfg.strict;
		if let Some(max) = self.faults.lock().max_open {
			if self.open_handles.load(Ordering::SeqCst) >= max {
				return Err(io::Error::other("too many open handles"));
			}
		}
		if exclusive && file.locked.swap(true, Ordering::SeqCst) {
			return Err(io::Error::new(
				io::ErrorKind::PermissionDenied,
				"the file is open in another handle",
			));
		}
		self.open_handles.fetch_add(1, Ordering::SeqCst);
		Ok(MemRawFile {
			state: Arc::clone(self),
			file: Arc::clone(file),
			closed: AtomicBool::new(false),
			exclusive,
		})
	}

	fn create_file(self: &Arc<Self>, name: &str) -> io::Result<MemRawFile> {
		self.begin(Kind::Create)?;
		let mut files = self.files.lock();
		if files.contains_key(name) {
			return Err(io::Error::new(io::ErrorKind::AlreadyExists, "the file is there"));
		}
		let file = Arc::new(MemFile::new(Vec::new()));
		// Opened before it is inserted, so a refusal leaves nothing behind.
		let handle = self.handle(&file, name == LOCK_NAME)?;
		files.insert(name.to_string(), file);
		self.creates.fetch_add(1, Ordering::SeqCst);
		self.next_op();
		Ok(handle)
	}

	fn open_file(self: &Arc<Self>, name: &str) -> io::Result<MemRawFile> {
		self.check_alive()?;
		let file = self
			.files
			.lock()
			.get(name)
			.cloned()
			.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))?;
		self.handle(&file, name == LOCK_NAME)
	}

	/// The operations of every file that were not flushed, in the order they were made.
	fn unflushed(files: &BTreeMap<String, Arc<MemFile>>) -> Vec<(u64, String, Op)> {
		let mut all = Vec::new();
		for (name, file) in files {
			for (seq, op) in &file.inner.lock().pending {
				all.push((*seq, name.clone(), op.clone()));
			}
		}
		all.sort_by_key(|(seq, _, _)| *seq);
		all
	}

	fn crash(&self, mode: CrashMode) -> MemRaw {
		let files = self.files.lock();
		let unflushed = Self::unflushed(&files);
		// Which operations survive, and in what form.
		let mut kept: Vec<(u64, String, Op)> = Vec::new();
		match mode {
			CrashMode::LoseUnsynced => {}
			CrashMode::Prefix(k) => kept.extend(unflushed.iter().take(k).cloned()),
			CrashMode::Tear(k, n) => {
				kept.extend(unflushed.iter().take(k).cloned());
				if let Some((
					seq,
					name,
					Op::Write {
						offset,
						data,
					},
				)) = unflushed.get(k)
				{
					let n = n.min(data.len());
					if n > 0 {
						kept.push((
							*seq,
							name.clone(),
							Op::Write {
								offset: *offset,
								data: data[..n].to_vec(),
							},
						));
					}
				}
			}
			CrashMode::LoseTear {
				lost,
				torn,
				n,
			} => {
				for (index, (seq, name, op)) in unflushed.iter().enumerate() {
					if index == lost {
						continue;
					}
					let op = match op {
						Op::Write {
							offset,
							data,
						} if index == torn => {
							let n = n.min(data.len());
							if n == 0 {
								continue;
							}
							Op::Write {
								offset: *offset,
								data: data[..n].to_vec(),
							}
						}
						other => other.clone(),
					};
					kept.push((*seq, name.clone(), op));
				}
			}
			CrashMode::ZeroTail => {
				// The length of every file survives, and the bytes that it grew by read as zeros: a
				// browser sizes a file when it is written past its end, and the blocks that were
				// never flushed are not there. What was already durable is not zeroed by a write
				// over it, which a browser never does: the write is lost, and the old bytes stay.
				// The line between the two is the durable length, which a truncation that is not
				// flushed lowers for every write after it.
				let mut reach: BTreeMap<&str, u64> = files
					.iter()
					.map(|(name, file)| (name.as_str(), file.inner.lock().durable.len() as u64))
					.collect();
				for (seq, name, op) in &unflushed {
					let reach = reach.entry(name.as_str()).or_default();
					match op {
						Op::Write {
							offset,
							data,
						} => {
							// The part of the write that is past the line, as zeros.
							let from = (*reach).max(*offset);
							let end = offset.saturating_add(data.len() as u64);
							if end > from {
								kept.push((
									*seq,
									name.clone(),
									Op::Write {
										offset: from,
										data: vec![0; usize::try_from(end - from).unwrap_or(0)],
									},
								));
							}
						}
						Op::SetLen(len) => {
							*reach = (*reach).min(*len);
							kept.push((*seq, name.clone(), op.clone()));
						}
					}
				}
			}
			CrashMode::Reorder(seed) => {
				for (seq, name, op) in &unflushed {
					let draw = splitmix64(seed ^ seq.wrapping_mul(0x9e37_79b9_7f4a_7c15)) % 4;
					let persists = match op {
						Op::Write {
							..
						} => draw != 0,
						Op::SetLen(_) => draw == 0,
					};
					if persists {
						kept.push((*seq, name.clone(), op.clone()));
					}
				}
			}
		}

		let image = MemState::new(self.cfg);
		let mut total = 0u64;
		{
			let mut out = image.files.lock();
			for (name, file) in files.iter() {
				let mut bytes = file.inner.lock().durable.clone();
				for (_, op_name, op) in &kept {
					if op_name == name {
						// A bound that the original passed cannot fail here; if it does the op
						// is a lost one.
						let _ = apply(&mut bytes, op);
					}
				}
				total += bytes.len() as u64;
				out.insert(name.clone(), Arc::new(MemFile::new(bytes)));
			}
		}
		image.total_bytes.store(total, Ordering::SeqCst);
		MemRaw {
			state: Arc::new(image),
		}
	}
}

fn splitmix64(mut x: u64) -> u64 {
	x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
	x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
	x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
	x ^ (x >> 31)
}

/// The slot files of a pool, in memory. Cloning gives another view of the same files, which is how
/// a test keeps a hand on the backend that a pool owns.
#[derive(Clone)]
pub(crate) struct MemRaw {
	state: Arc<MemState>,
}

impl Default for MemRaw {
	fn default() -> Self {
		Self::new()
	}
}

impl std::fmt::Debug for MemRaw {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("MemRaw").field("files", &self.state.files.lock().len()).finish()
	}
}

impl MemRaw {
	pub(crate) fn new() -> Self {
		Self::with_config(MemConfig::default())
	}

	pub(crate) fn with_config(cfg: MemConfig) -> Self {
		Self {
			state: Arc::new(MemState::new(cfg)),
		}
	}
}

/// What the tests of the pool, of the engine and of a browser do to a backend and look at in it:
/// the faults, the hangs, and what was written. Nothing outside a test calls them.
#[allow(dead_code, reason = "the hooks of the tests: each test uses some of them")]
impl MemRaw {
	pub(crate) fn config(&self) -> MemConfig {
		self.state.cfg
	}

	// ---- faults -----------------------------------------------------------------------------

	/// Fails the write that comes after `n` more writes, once.
	pub(crate) fn fail_write_after(&self, n: usize) {
		self.state.faults.lock().fail_write_after = Some(n);
	}

	/// Fails the flush that comes after `n` more flushes, once.
	pub(crate) fn fail_flush_after(&self, n: usize) {
		self.state.faults.lock().fail_flush_after = Some(n);
	}

	/// Fails the truncation that comes after `n` more, once.
	pub(crate) fn fail_truncate_after(&self, n: usize) {
		self.state.faults.lock().fail_truncate_after = Some(n);
	}

	/// Every write that follows writes at most `max` bytes and says so (a `max` of 0 is a write
	/// that makes no progress). `None` ends it.
	pub(crate) fn short_write(&self, max: Option<usize>) {
		self.state.faults.lock().short_write = max;
	}

	/// The next write puts `prefix` of its bytes in the file and then fails, once.
	pub(crate) fn write_then_fail(&self, prefix: usize) {
		self.state.faults.lock().write_then_fail = Some(prefix);
	}

	/// Growth past `bytes` in all the files together fails with `StorageFull`. `None` ends it.
	pub(crate) fn quota(&self, bytes: Option<u64>) {
		self.state.faults.lock().quota = bytes;
	}

	/// An open beyond `n` handles at once fails. `None` ends it.
	pub(crate) fn max_open(&self, n: Option<usize>) {
		self.state.faults.lock().max_open = n;
	}

	/// Kills the backend after `n` more operations that change something: from then on every
	/// operation fails, as it would not have happened after a crash.
	pub(crate) fn die_after(&self, n: u64) {
		self.state.faults.lock().die_after = Some(n);
	}

	pub(crate) fn is_dead(&self) -> bool {
		self.state.faults.lock().dead
	}

	/// From now on a create, open or list says "not yet" `n` times before it completes.
	pub(crate) fn set_pending_polls(&self, n: usize) {
		self.state.pending_polls.store(n, Ordering::SeqCst);
	}

	/// Ends every fault and revives a killed backend. The hangs are not faults: `clear_hangs` ends
	/// them.
	pub(crate) fn clear_faults(&self) {
		*self.state.faults.lock() = Faults::default();
	}

	/// The `n` calls of `list`, `open`, `create` and `open_lock` that are made from now on complete
	/// as they would, and every later one hangs: its future returns `Pending` for ever, does not
	/// wake its task, and does not do its work, so nothing is created, opened or locked. A request
	/// that the browser never answers, for the tests of a future that is dropped while it waits:
	/// whatever such a test finds open or locked afterwards was left by the code under test, and
	/// not by this backend. The calls are counted when the future is made, not when it is polled.
	pub(crate) fn hang_after(&self, n: usize) {
		self.state.hangs.lock().after = Some(n);
	}

	/// Every `open` and `create` of `name` hangs from now on, as under `hang_after`. The lock is
	/// the name [`LOCK_NAME`], and `list` has none.
	pub(crate) fn hang_name(&self, name: &str) {
		self.state.hangs.lock().names.insert(name.to_string());
	}

	/// Ends every hang. Futures that are hanging stay hung.
	pub(crate) fn clear_hangs(&self) {
		*self.state.hangs.lock() = Hangs::default();
	}

	/// The futures that were made hung, ever.
	pub(crate) fn hung_futures(&self) -> usize {
		self.state.hung.load(Ordering::SeqCst)
	}

	// ---- what a test looks at ---------------------------------------------------------------

	/// The operations that changed something so far (creates, writes, truncations and flushes)
	/// that passed the gate of `die_after`: after `die_after(n)` is set with `ops()` at `m`, the
	/// backend is killed at the operation that makes `ops()` `m + n + 1`.
	pub(crate) fn ops(&self) -> u64 {
		self.state.ops_done.load(Ordering::SeqCst)
	}

	pub(crate) fn writes(&self) -> u64 {
		self.state.writes.load(Ordering::SeqCst)
	}

	pub(crate) fn flushes(&self) -> u64 {
		self.state.flushes.load(Ordering::SeqCst)
	}

	pub(crate) fn creates(&self) -> u64 {
		self.state.creates.load(Ordering::SeqCst)
	}

	/// Handles that are open now.
	pub(crate) fn open_handles(&self) -> usize {
		self.state.open_handles.load(Ordering::SeqCst)
	}

	/// Handles that were dropped without a close.
	pub(crate) fn leaked(&self) -> usize {
		self.state.leaked.load(Ordering::SeqCst)
	}

	/// The names of the files, the lock included.
	pub(crate) fn names(&self) -> Vec<String> {
		self.state.files.lock().keys().cloned().collect()
	}

	/// What a read of the file sees.
	pub(crate) fn image(&self, name: &str) -> Option<Vec<u8>> {
		self.state.files.lock().get(name).map(|file| file.inner.lock().current.clone())
	}

	/// What a flush has made durable.
	pub(crate) fn durable_image(&self, name: &str) -> Option<Vec<u8>> {
		self.state.files.lock().get(name).map(|file| file.inner.lock().durable.clone())
	}

	/// How many operations of the file have not been flushed.
	pub(crate) fn pending_ops(&self, name: &str) -> usize {
		self.state.files.lock().get(name).map_or(0, |file| file.inner.lock().pending.len())
	}

	/// The number of operations, over all the files, that a crash may still lose.
	pub(crate) fn unflushed_ops(&self) -> usize {
		MemState::unflushed(&self.state.files.lock()).len()
	}

	/// Sets the content of a file, durably, creating it: how a test builds a state that the pool
	/// found after a crash.
	pub(crate) fn put(&self, name: &str, bytes: &[u8]) {
		let mut files = self.state.files.lock();
		let old = files.get(name).map_or(0, |file| file.inner.lock().current.len() as u64);
		self.state.total_bytes.fetch_sub(old, Ordering::SeqCst);
		self.state.total_bytes.fetch_add(bytes.len() as u64, Ordering::SeqCst);
		files.insert(name.to_string(), Arc::new(MemFile::new(bytes.to_vec())));
	}

	/// Removes a file: a slot that a browser lost.
	pub(crate) fn remove(&self, name: &str) {
		if let Some(file) = self.state.files.lock().remove(name) {
			let len = file.inner.lock().current.len() as u64;
			self.state.total_bytes.fetch_sub(len, Ordering::SeqCst);
		}
	}

	/// A new backend with what a crash would leave of this one, handles closed and faults clear.
	/// This one is not touched, so a test can crash one state in every way.
	pub(crate) fn crash(&self, mode: CrashMode) -> MemRaw {
		self.state.crash(mode)
	}

	/// A copy of everything, flushed or not, as it is now: a crash that loses nothing.
	pub(crate) fn snapshot(&self) -> MemRaw {
		self.state.crash(CrashMode::Prefix(usize::MAX))
	}
}

/// An open file of a [`MemRaw`].
pub(crate) struct MemRawFile {
	state: Arc<MemState>,
	file: Arc<MemFile>,
	closed: AtomicBool,
	exclusive: bool,
}

impl MemRawFile {
	fn open_or_err(&self) -> io::Result<()> {
		if self.closed.load(Ordering::SeqCst) {
			return Err(io::Error::new(io::ErrorKind::NotConnected, "the handle is closed"));
		}
		Ok(())
	}

	/// Puts `data` at `offset`, and numbers it. The file lock is held, so the numbers of one file
	/// are in the order of its writes.
	fn put_write(&self, inner: &mut FileInner, offset: u64, data: &[u8]) -> io::Result<()> {
		let op = Op::Write {
			offset,
			data: data.to_vec(),
		};
		let before = inner.current.len() as u64;
		apply(&mut inner.current, &op)?;
		self.track(inner, before, op);
		Ok(())
	}

	fn track(&self, inner: &mut FileInner, before: u64, op: Op) {
		let after = inner.current.len() as u64;
		if after >= before {
			self.state.total_bytes.fetch_add(after - before, Ordering::SeqCst);
		} else {
			self.state.total_bytes.fetch_sub(before - after, Ordering::SeqCst);
		}
		let seq = self.state.next_op();
		if self.state.cfg.track_unsynced {
			inner.pending.push((seq, op));
		} else {
			// Nothing is lost by a crash here: the operation is durable as soon as it is made.
			let _ = apply(&mut inner.durable, &op);
		}
	}

	fn growth(&self, inner: &FileInner, end: u64) -> io::Result<()> {
		let quota = self.state.faults.lock().quota;
		if let Some(quota) = quota {
			let len = inner.current.len() as u64;
			if end > len {
				let total = self.state.total_bytes.load(Ordering::SeqCst);
				if total.saturating_add(end - len) > quota {
					return Err(io::Error::new(
						io::ErrorKind::StorageFull,
						"the quota of the memory backend is used",
					));
				}
			}
		}
		Ok(())
	}
}

impl RawFile for MemRawFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
		self.open_or_err()?;
		self.state.check_alive()?;
		let inner = self.file.inner.lock();
		let Some(rest) = usize::try_from(offset).ok().and_then(|start| inner.current.get(start..))
		else {
			return Ok(0);
		};
		let n = rest.len().min(buf.len());
		buf[..n].copy_from_slice(&rest[..n]);
		Ok(n)
	}

	fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
		self.open_or_err()?;
		self.state.begin(Kind::Write)?;
		if buf.is_empty() {
			return Ok(0);
		}
		let (cap, then_fail) = {
			let mut faults = self.state.faults.lock();
			(faults.short_write, faults.write_then_fail.take())
		};
		let mut inner = self.file.inner.lock();
		let n = cap.map_or(buf.len(), |cap| cap.min(buf.len()));
		let end = offset.checked_add(n as u64).ok_or_else(too_large)?;
		self.growth(&inner, end)?;
		if let Some(prefix) = then_fail {
			let prefix = prefix.min(n);
			if prefix > 0 {
				self.put_write(&mut inner, offset, &buf[..prefix])?;
				self.state.writes.fetch_add(1, Ordering::SeqCst);
			}
			return Err(injected("a write, after a part of it landed"));
		}
		self.put_write(&mut inner, offset, &buf[..n])?;
		self.state.writes.fetch_add(1, Ordering::SeqCst);
		Ok(n)
	}

	fn set_len(&self, len: u64) -> io::Result<()> {
		self.open_or_err()?;
		self.state.begin(Kind::SetLen)?;
		let mut inner = self.file.inner.lock();
		self.growth(&inner, len)?;
		let op = Op::SetLen(len);
		let before = inner.current.len() as u64;
		apply(&mut inner.current, &op)?;
		self.track(&mut inner, before, op);
		self.state.writes.fetch_add(1, Ordering::SeqCst);
		Ok(())
	}

	fn size(&self) -> io::Result<u64> {
		self.open_or_err()?;
		self.state.check_alive()?;
		Ok(self.file.inner.lock().current.len() as u64)
	}

	fn flush(&self) -> io::Result<()> {
		self.open_or_err()?;
		self.state.begin(Kind::Flush)?;
		let mut inner = self.file.inner.lock();
		let pending = std::mem::take(&mut inner.pending);
		for (_, op) in &pending {
			apply(&mut inner.durable, op)?;
		}
		self.state.flushes.fetch_add(1, Ordering::SeqCst);
		Ok(())
	}

	fn close(&self) -> io::Result<()> {
		if !self.closed.swap(true, Ordering::SeqCst) {
			if self.exclusive {
				self.file.locked.store(false, Ordering::SeqCst);
			}
			self.state.open_handles.fetch_sub(1, Ordering::SeqCst);
		}
		Ok(())
	}
}

impl Drop for MemRawFile {
	fn drop(&mut self) {
		if !self.closed.load(Ordering::SeqCst) {
			// A handle that nobody closed: it keeps its file locked, if it locked it, and is
			// counted, so that a test can say that the pool leaked it.
			self.state.leaked.fetch_add(1, Ordering::SeqCst);
		}
	}
}

/// A result that is ready after `remaining` pending polls, and was made at the first poll.
struct Delayed<T> {
	op: Option<Box<dyn FnOnce() -> T + Send>>,
	result: Option<T>,
	remaining: usize,
}

impl<T> Unpin for Delayed<T> {}

impl<T> Future for Delayed<T> {
	type Output = T;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
		if let Some(op) = self.op.take() {
			self.result = Some(op());
		}
		if self.remaining > 0 {
			self.remaining -= 1;
			cx.waker().wake_by_ref();
			return Poll::Pending;
		}
		match self.result.take() {
			Some(result) => Poll::Ready(result),
			None => Poll::Pending,
		}
	}
}

/// A result that never comes: pending for ever, and nothing wakes the task.
struct Hung<T>(std::marker::PhantomData<fn() -> T>);

impl<T> Unpin for Hung<T> {}

impl<T> Future for Hung<T> {
	type Output = T;

	fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<T> {
		Poll::Pending
	}
}

impl MemRaw {
	/// Whether a call for `name` hangs, which is decided, and counted, when it is made.
	fn hangs(&self, name: &str) -> bool {
		let mut hangs = self.state.hangs.lock();
		let hang = hangs.names.contains(name)
			|| match hangs.after.as_mut() {
				Some(0) => true,
				Some(left) => {
					*left -= 1;
					false
				}
				None => false,
			};
		if hang {
			self.state.hung.fetch_add(1, Ordering::SeqCst);
		}
		hang
	}

	fn delayed<T: Send + 'static>(
		&self,
		name: &str,
		op: impl FnOnce() -> T + Send + 'static,
	) -> RawFut<'static, T> {
		if self.hangs(name) {
			return Box::pin(Hung(std::marker::PhantomData));
		}
		Box::pin(Delayed {
			op: Some(Box::new(op)),
			result: None,
			remaining: self.state.pending_polls.load(Ordering::SeqCst),
		})
	}
}

impl Raw for MemRaw {
	type File = MemRawFile;

	fn list(&self) -> RawFut<'_, io::Result<Vec<String>>> {
		let state = Arc::clone(&self.state);
		self.delayed("", move || {
			state.check_alive()?;
			Ok(state.files.lock().keys().filter(|name| *name != LOCK_NAME).cloned().collect())
		})
	}

	fn open(&self, name: &str) -> RawFut<'_, io::Result<MemRawFile>> {
		let state = Arc::clone(&self.state);
		let name = name.to_string();
		self.delayed(&name.clone(), move || state.open_file(&name))
	}

	fn create(&self, name: &str) -> RawFut<'_, io::Result<MemRawFile>> {
		let state = Arc::clone(&self.state);
		let name = name.to_string();
		self.delayed(&name.clone(), move || state.create_file(&name))
	}

	fn open_lock(&self) -> RawFut<'_, io::Result<MemRawFile>> {
		let state = Arc::clone(&self.state);
		self.delayed(LOCK_NAME, move || {
			state.check_alive()?;
			let existing = state.files.lock().get(LOCK_NAME).cloned();
			match existing {
				Some(file) => state.handle(&file, true),
				None => state.create_file(LOCK_NAME),
			}
		})
	}

	fn can_create_sync(&self) -> bool {
		!self.state.cfg.strict && self.state.pending_polls.load(Ordering::SeqCst) == 0
	}

	fn create_sync(&self, name: &str) -> Option<io::Result<MemRawFile>> {
		if !self.can_create_sync() {
			return None;
		}
		Some(self.state.create_file(name))
	}

	fn crash_image(&self, mode: CrashMode) -> Option<Self> {
		Some(self.crash(mode))
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use super::*;
	use crate::vfs::pool::poll_ready;

	fn create(raw: &MemRaw, name: &str) -> MemRawFile {
		poll_ready(raw.create(name)).expect("ready").expect("create")
	}

	fn write_all(file: &MemRawFile, offset: u64, bytes: &[u8]) {
		assert_eq!(file.write_at(offset, bytes).unwrap(), bytes.len());
	}

	fn read_all(raw: &MemRaw, name: &str) -> Vec<u8> {
		raw.image(name).unwrap()
	}

	#[test]
	fn reads_see_unflushed_writes_and_a_crash_does_not() {
		let raw = MemRaw::new();
		let file = create(&raw, "a");
		write_all(&file, 0, b"hello");
		write_all(&file, 8, b"!");
		assert_eq!(file.size().unwrap(), 9);
		let mut buf = [9u8; 9];
		assert_eq!(file.read_at(0, &mut buf).unwrap(), 9);
		assert_eq!(&buf, b"hello\0\0\0!");
		assert_eq!(file.read_at(9, &mut buf).unwrap(), 0);
		assert_eq!(file.read_at(u64::MAX, &mut buf).unwrap(), 0);
		assert_eq!(raw.pending_ops("a"), 2);

		assert_eq!(read_all(&raw.crash(CrashMode::LoseUnsynced), "a"), b"");
		file.flush().unwrap();
		assert_eq!(raw.pending_ops("a"), 0);
		assert_eq!(raw.durable_image("a").unwrap(), b"hello\0\0\0!");
		write_all(&file, 0, b"J");
		file.set_len(3).unwrap();
		assert_eq!(read_all(&raw, "a"), b"Jel");
		assert_eq!(read_all(&raw.crash(CrashMode::LoseUnsynced), "a"), b"hello\0\0\0!");
		assert_eq!(read_all(&raw.crash(CrashMode::Prefix(1)), "a"), b"Jello\0\0\0!");
		assert_eq!(read_all(&raw.crash(CrashMode::Prefix(2)), "a"), b"Jel");
		assert_eq!(read_all(&raw.crash(CrashMode::Prefix(99)), "a"), b"Jel");
		assert_eq!(read_all(&raw.snapshot(), "a"), b"Jel");
		// A shrink and then a grow does not bring the old bytes back.
		file.set_len(0).unwrap();
		file.set_len(4).unwrap();
		assert_eq!(read_all(&raw, "a"), [0, 0, 0, 0]);
	}

	#[test]
	fn the_crash_modes_keep_what_they_say() {
		let raw = MemRaw::new();
		let a = create(&raw, "a");
		let b = create(&raw, "b");
		write_all(&a, 0, b"AAAA");
		a.flush().unwrap();
		write_all(&a, 4, b"1111"); // op 0 of the unflushed
		write_all(&b, 0, b"2222"); // op 1
		a.set_len(6).unwrap(); // op 2
		write_all(&b, 4, b"3333"); // op 3

		// A prefix is in the order of the writes of every file together.
		let c = raw.crash(CrashMode::Prefix(2));
		assert_eq!(read_all(&c, "a"), b"AAAA1111");
		assert_eq!(read_all(&c, "b"), b"2222");
		let c = raw.crash(CrashMode::Prefix(3));
		assert_eq!(read_all(&c, "a"), b"AAAA11");

		// A tear keeps the first bytes of the next write.
		let c = raw.crash(CrashMode::Tear(1, 2));
		assert_eq!(read_all(&c, "a"), b"AAAA1111");
		assert_eq!(read_all(&c, "b"), b"22");
		let c = raw.crash(CrashMode::Tear(0, 3));
		assert_eq!(read_all(&c, "a"), b"AAAA111");
		assert_eq!(read_all(&c, "b"), b"");
		// A tear at a truncation keeps nothing of it.
		let c = raw.crash(CrashMode::Tear(2, 1));
		assert_eq!(read_all(&c, "a"), b"AAAA1111");

		// An earlier write lost and a later one torn, both in one crash.
		let c = raw.crash(CrashMode::LoseTear {
			lost: 0,
			torn: 3,
			n: 2,
		});
		assert_eq!(read_all(&c, "a"), b"AAAA\0\0");
		assert_eq!(read_all(&c, "b"), b"222233");
		let c = raw.crash(CrashMode::LoseTear {
			lost: 1,
			torn: 1,
			n: 2,
		});
		assert_eq!(read_all(&c, "b"), [0, 0, 0, 0, b'3', b'3', b'3', b'3']);

		// Zero tail: the lengths survive and the new bytes do not.
		let c = raw.crash(CrashMode::ZeroTail);
		assert_eq!(read_all(&c, "a"), b"AAAA\0\0");
		assert_eq!(read_all(&c, "b"), [0u8; 8]);

		// Reorder is deterministic for a seed, and keeps what was flushed.
		for seed in 0..32 {
			let first = raw.crash(CrashMode::Reorder(seed));
			let second = raw.crash(CrashMode::Reorder(seed));
			for name in ["a", "b"] {
				assert_eq!(read_all(&first, name), read_all(&second, name));
			}
			assert!(read_all(&first, "a").starts_with(b"AAAA") || read_all(&first, "a").len() < 4);
		}
		let seen: std::collections::BTreeSet<Vec<u8>> =
			(0..64).map(|seed| read_all(&raw.crash(CrashMode::Reorder(seed)), "b")).collect();
		assert!(seen.len() > 1, "the seeds make different crashes");

		// The original is untouched by every crash.
		assert_eq!(read_all(&raw, "a"), b"AAAA11");
		assert_eq!(read_all(&raw, "b"), b"22223333");
	}

	#[test]
	fn zero_tail_zeroes_only_what_grew_and_keeps_the_durable_bytes_below_it() {
		let raw = MemRaw::new();
		let a = create(&raw, "a");
		write_all(&a, 0, b"AAAA");
		a.flush().unwrap();
		write_all(&a, 0, b"BB"); // in place: below the durable end
		write_all(&a, 2, b"CCCC"); // straddles the durable end
		let c = raw.crash(CrashMode::ZeroTail);
		assert_eq!(
			read_all(&c, "a"),
			b"AAAA\0\0",
			"an overwrite of durable bytes is lost whole, and only the growth reads as zeros"
		);

		// A shrink that is not flushed lowers the end that counts for the writes after it.
		let b = create(&raw, "b");
		write_all(&b, 0, b"AAAA");
		b.flush().unwrap();
		b.set_len(1).unwrap();
		write_all(&b, 0, b"XY"); // byte 0 is below the lowered end, byte 1 is past it
		let c = raw.crash(CrashMode::ZeroTail);
		assert_eq!(read_all(&c, "b"), b"A\0");
		b.set_len(0).unwrap();
		write_all(&b, 0, b"BB");
		assert_eq!(read_all(&raw.crash(CrashMode::ZeroTail), "b"), [0u8, 0]);

		// A file that never was flushed has nothing below its end: all of it is growth.
		let n = create(&raw, "n");
		write_all(&n, 0, b"new");
		assert_eq!(read_all(&raw.crash(CrashMode::ZeroTail), "n"), [0u8; 3]);
	}

	#[test]
	fn a_hung_future_never_completes_wakes_nothing_and_does_nothing() {
		use std::sync::atomic::AtomicUsize;
		use std::task::Wake;

		struct Counting(AtomicUsize);
		impl Wake for Counting {
			fn wake(self: Arc<Self>) {
				self.0.fetch_add(1, Ordering::SeqCst);
			}
		}
		let wakes = Arc::new(Counting(AtomicUsize::new(0)));
		let waker = std::task::Waker::from(Arc::clone(&wakes));
		let mut cx = Context::from_waker(&waker);

		let raw = MemRaw::new();
		// The first two calls complete, the third and every one after it hang.
		raw.hang_after(2);
		create(&raw, "p00000001").close().unwrap();
		create(&raw, "p00000002").close().unwrap();
		let mut hung = raw.create("p00000003");
		for _ in 0..1000 {
			assert!(hung.as_mut().poll(&mut cx).is_pending());
		}
		assert_eq!(wakes.0.load(Ordering::SeqCst), 0, "a hung future does not wake itself");
		assert!(!raw.names().contains(&"p00000003".to_string()), "and does not do its work");
		assert_eq!(raw.hung_futures(), 1);
		drop(hung);
		assert_eq!(raw.open_handles(), 0);
		assert_eq!(raw.leaked(), 0, "nothing arrived, so nothing can leak");
		raw.clear_hangs();
		create(&raw, "p00000003").close().unwrap();

		// By name: only the calls that ask for that name hang.
		raw.hang_name("p00000009");
		let mut other = raw.open("p00000009");
		assert!(other.as_mut().poll(&mut cx).is_pending());
		assert!(other.as_mut().poll(&mut cx).is_pending());
		poll_ready(raw.open("p00000001")).unwrap().unwrap().close().unwrap();
		raw.hang_name(LOCK_NAME);
		let mut hung_lock = raw.open_lock();
		assert!(hung_lock.as_mut().poll(&mut cx).is_pending());
		assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
	}

	#[test]
	fn a_backend_that_does_not_track_loses_nothing() {
		let raw = MemRaw::with_config(MemConfig {
			track_unsynced: false,
			..MemConfig::default()
		});
		let file = create(&raw, "a");
		write_all(&file, 0, b"kept");
		assert_eq!(raw.pending_ops("a"), 0);
		assert_eq!(read_all(&raw.crash(CrashMode::LoseUnsynced), "a"), b"kept");
	}

	#[test]
	fn faults_fire_once_where_they_are_set() {
		let raw = MemRaw::new();
		let file = create(&raw, "a");

		raw.fail_write_after(1);
		write_all(&file, 0, b"1");
		assert!(file.write_at(1, b"2").is_err());
		write_all(&file, 1, b"3");
		assert_eq!(read_all(&raw, "a"), b"13");

		raw.fail_flush_after(0);
		assert!(file.flush().is_err());
		file.flush().unwrap();
		raw.fail_truncate_after(0);
		assert!(file.set_len(0).is_err());
		file.set_len(1).unwrap();

		// A short write says how much it wrote.
		raw.short_write(Some(2));
		assert_eq!(file.write_at(0, b"abcdef").unwrap(), 2);
		raw.short_write(None);
		assert_eq!(file.write_at(0, b"abcdef").unwrap(), 6);

		// A write that fails after a part of it landed.
		raw.write_then_fail(2);
		assert!(file.write_at(0, b"WXYZ").is_err());
		assert_eq!(&read_all(&raw, "a")[..6], b"WXcdef");
	}

	#[test]
	fn the_quota_refuses_growth_with_storage_full() {
		let raw = MemRaw::new();
		let file = create(&raw, "a");
		raw.quota(Some(10));
		write_all(&file, 0, b"0123456789");
		assert_eq!(file.write_at(5, b"abc").unwrap(), 3, "overwriting is not growth");
		assert_eq!(file.write_at(10, b"x").unwrap_err().kind(), io::ErrorKind::StorageFull);
		assert_eq!(file.set_len(11).unwrap_err().kind(), io::ErrorKind::StorageFull);
		file.set_len(4).unwrap();
		write_all(&file, 4, b"456789");
		let other = create(&raw, "b");
		assert_eq!(other.write_at(0, b"1").unwrap_err().kind(), io::ErrorKind::StorageFull);
		raw.quota(None);
		write_all(&other, 0, b"1");
	}

	#[test]
	fn a_killed_backend_stops_everything_after_the_nth_operation() {
		let raw = MemRaw::new();
		let file = create(&raw, "a");
		raw.die_after(2);
		write_all(&file, 0, b"1");
		file.flush().unwrap();
		assert!(!raw.is_dead());
		assert!(file.write_at(1, b"2").is_err());
		assert!(raw.is_dead());
		assert!(file.flush().is_err());
		assert!(file.set_len(0).is_err());
		let mut buf = [0u8; 1];
		assert!(file.read_at(0, &mut buf).is_err());
		assert!(poll_ready(raw.create("b")).unwrap().is_err());
		assert!(poll_ready(raw.open("a")).unwrap().is_err());
		file.close().unwrap();
		// What the crash left is what was done before the kill.
		let after = raw.crash(CrashMode::LoseUnsynced);
		assert_eq!(read_all(&after, "a"), b"1");
		assert!(!after.is_dead());
		raw.clear_faults();
		assert!(!raw.is_dead());
	}

	#[test]
	fn handles_are_counted_closed_and_leaked() {
		let raw = MemRaw::new();
		let a = create(&raw, "a");
		let b = create(&raw, "b");
		assert_eq!(raw.open_handles(), 2);
		a.close().unwrap();
		a.close().unwrap();
		assert_eq!(raw.open_handles(), 1);
		assert!(a.size().is_err(), "a closed handle is not connected");
		assert_eq!(a.size().unwrap_err().kind(), io::ErrorKind::NotConnected);
		assert_eq!(raw.leaked(), 0);
		drop(b);
		assert_eq!(raw.leaked(), 1);
		drop(a);
		assert_eq!(raw.leaked(), 1);

		raw.max_open(Some(1));
		assert!(poll_ready(raw.open("a")).unwrap().is_err());
		raw.max_open(None);
		let again = poll_ready(raw.open("a")).unwrap().unwrap();
		again.close().unwrap();
		assert!(poll_ready(raw.create("a")).unwrap().is_err(), "a name is created once");
		assert_eq!(
			poll_ready(raw.open("none")).unwrap().err().map(|e| e.kind()),
			Some(io::ErrorKind::NotFound)
		);
	}

	#[test]
	fn the_lock_is_exclusive_and_a_leaked_handle_keeps_a_strict_file() {
		let raw = MemRaw::new();
		let lock = poll_ready(raw.open_lock()).unwrap().unwrap();
		assert_eq!(
			poll_ready(raw.open_lock()).unwrap().err().map(|e| e.kind()),
			Some(io::ErrorKind::PermissionDenied)
		);
		assert!(!raw.names().is_empty() && poll_ready(raw.list()).unwrap().unwrap().is_empty());
		lock.close().unwrap();
		poll_ready(raw.open_lock()).unwrap().unwrap().close().unwrap();

		// Non-strict: two handles on one file are fine, and a leak releases nothing that was
		// locked. Strict: the second open fails, and so does one after a leak.
		let slot = create(&raw, "p00000001");
		let second = poll_ready(raw.open("p00000001")).unwrap().unwrap();
		second.close().unwrap();
		slot.close().unwrap();

		let strict = MemRaw::with_config(MemConfig::strict());
		let slot = crate::vfs::pool::poll_ready(strict.create("p00000001")).unwrap().unwrap();
		assert!(strict.create_sync("p00000002").is_none(), "no inline provisioning when strict");
		assert_eq!(
			poll_ready(strict.open("p00000001")).unwrap().err().map(|e| e.kind()),
			Some(io::ErrorKind::PermissionDenied)
		);
		drop(slot);
		assert_eq!(
			poll_ready(strict.open("p00000001")).unwrap().err().map(|e| e.kind()),
			Some(io::ErrorKind::PermissionDenied),
			"a dropped handle keeps the file locked"
		);
		assert_eq!(strict.leaked(), 1);
	}

	#[test]
	fn a_strict_future_that_is_dropped_half_way_leaves_a_handle_that_nobody_closes() {
		use std::task::Waker;

		let mut cx = Context::from_waker(Waker::noop());

		let strict = MemRaw::with_config(MemConfig::strict());
		let mut fut = strict.create("p00000001");
		assert!(fut.as_mut().poll(&mut cx).is_pending(), "pending once");
		assert!(
			strict.names().contains(&"p00000001".to_string()),
			"the file exists at the first poll"
		);
		assert_eq!(strict.open_handles(), 1);
		drop(fut);
		assert_eq!(strict.leaked(), 1, "the late handle was not closed");

		// A future that is polled to the end hands the handle over.
		let mut fut = strict.create("p00000002");
		assert!(fut.as_mut().poll(&mut cx).is_pending());
		let Poll::Ready(file) = fut.as_mut().poll(&mut cx) else {
			panic!("ready after one pending poll")
		};
		file.unwrap().close().unwrap();
	}

	#[test]
	fn a_file_that_is_too_large_is_refused_and_not_allocated() {
		let raw = MemRaw::new();
		let file = create(&raw, "a");
		assert_eq!(file.set_len(u64::MAX).unwrap_err().kind(), io::ErrorKind::StorageFull);
		assert_eq!(
			file.write_at(u64::MAX - 1, b"abc").unwrap_err().kind(),
			io::ErrorKind::StorageFull
		);
		assert_eq!(file.write_at(1 << 40, b"abc").unwrap_err().kind(), io::ErrorKind::StorageFull);
		assert_eq!(file.size().unwrap(), 0);
	}

	#[test]
	fn put_and_remove_build_states_for_the_tests() {
		let raw = MemRaw::new();
		raw.put("p00000001", b"abc");
		assert_eq!(raw.durable_image("p00000001").unwrap(), b"abc");
		raw.put("p00000001", b"z");
		assert_eq!(read_all(&raw, "p00000001"), b"z");
		assert_eq!(poll_ready(raw.list()).unwrap().unwrap(), vec!["p00000001".to_string()]);
		raw.remove("p00000001");
		assert!(raw.image("p00000001").is_none());
		assert_eq!(raw.unflushed_ops(), 0);
	}
}
