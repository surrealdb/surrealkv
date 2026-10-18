//! Tests of the engine that run in a dedicated worker of a real headless Chrome, where
//! `wasm32-unknown-unknown` has no threads, no `std::time` clock and no OS random source.
//!
//! They are tests of the crate's own `pub(crate)` items (the runtime shim, the clock, the error
//! handler), which an integration test under `tests/` cannot reach, so they are unit tests that
//! `wasm-pack` runs through the library target.
//!
//! Run them, from the repository root, with
//!
//! ```text
//! WASM_BINDGEN_TEST_TIMEOUT=300 WASM_BINDGEN_USE_DEDICATED_WORKER=1 \
//!     wasm-pack test --headless --chrome -- --lib
//! ```
//!
//! The test binary links the whole engine, zstd's C code included. On macOS the system `ar` writes
//! an empty archive for it, so point the C toolchain at LLVM's first:
//!
//! ```text
//! export CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang
//! export AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
//! ```
//!
//! No `RUSTFLAGS` are needed: the tests below that draw random bytes are the proof of that, for
//! both `getrandom` versions in the graph.
//!
//! A panic or a trap in one test ends the instance, and with it the tests that did not run yet.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use wasm_bindgen::JsCast;
use wasm_bindgen_test::*;

use crate::clock::{DefaultLogicalClock, LogicalClock};
use crate::error::{BackgroundErrorHandler, BackgroundErrorReason};
use crate::stall::{StallCounts, StallThresholds, WriteStallController, WriteStallCountProvider};
use crate::{rt, Error, Options, TreeBuilder};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// `getrandom` 0.4 is what draws the per-file salt and the database id.
#[wasm_bindgen_test]
fn getrandom_04_draws_bytes_without_rustflags() {
	let mut first = [0u8; 32];
	let mut second = [0u8; 32];
	getrandom_04::fill(&mut first).expect("the browser has a random source");
	getrandom_04::fill(&mut second).expect("the browser has a random source");
	assert_ne!(first, [0u8; 32]);
	assert_ne!(first, second);
}

/// `rand` 0.9 seeds its generator through `getrandom` 0.3, which is the one that used to need
/// `--cfg getrandom_backend="wasm_js"` before 0.3.4.
#[wasm_bindgen_test]
fn rand_draws_bytes_without_rustflags() {
	let first: [u64; 4] = rand::random();
	let second: [u64; 4] = rand::random();
	assert_ne!(first, [0u64; 4]);
	assert_ne!(first, second);
}

// ---------------------------------------------------------------------------------------------
// The runtime shim: what only a browser can show. The behaviour of spawn, abort, sleep, timeout and
// Instant, which native tests check too, is tested in `rt::tests`, which run here as well.
// ---------------------------------------------------------------------------------------------

/// The measured starvation: a pending OPFS call is delivered by a task of the event loop, so it
/// does not resolve while a loop yields through the microtask queue only, however long it runs.
/// `rt::relax` yields through a macrotask, and lets it through.
#[wasm_bindgen_test]
async fn relax_lets_a_pending_opfs_call_through_and_yield_now_does_not() {
	let resolved = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&resolved);
	let call = rt::spawn(async move {
		crate::storage::opfs::get_opfs_root().await.expect("the worker has an OPFS root");
		flag.store(true, Ordering::SeqCst);
	});

	// The task is polled during the first yield, and starts the call there.
	for _ in 0..5000 {
		rt::yield_now().await;
	}
	assert!(
		!resolved.load(Ordering::SeqCst),
		"an OPFS call resolved during a loop that never reached the event loop"
	);

	// The browser takes a few milliseconds of wall time to answer, which a tight loop of yields
	// covers in a few thousand turns: bound it by time, not by turns.
	let started = rt::Instant::now();
	let mut turns = 0u32;
	while !resolved.load(Ordering::SeqCst) && started.elapsed() < Duration::from_secs(10) {
		rt::relax().await;
		turns += 1;
	}
	assert!(
		resolved.load(Ordering::SeqCst),
		"{turns} macrotask yields in {:?} did not deliver the call",
		started.elapsed()
	);
	console_log!("relax delivered the OPFS call after {turns} turns, {:?}", started.elapsed());
	call.await.expect("the task finishes");
}

/// `relax` is a `MessageChannel` message and not a `setTimeout(0)`, whose chained calls the browser
/// clamps to 4 ms or more each: 500 of those would take 2 s or more.
#[wasm_bindgen_test]
async fn relax_is_not_a_clamped_timer() {
	let started = rt::Instant::now();
	for _ in 0..500 {
		rt::relax().await;
	}
	let elapsed = started.elapsed();
	assert!(elapsed < Duration::from_millis(800), "500 yields took {elapsed:?}");
}

/// A timer is a real timer: a task that sleeps lets a task that does not run in the meantime.
#[wasm_bindgen_test]
async fn a_sleeping_task_does_not_block_the_worker() {
	let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
	let counter = Arc::clone(&ticks);
	let ticker = rt::spawn(async move {
		for _ in 0..10 {
			rt::sleep(Duration::from_millis(2)).await;
			counter.fetch_add(1, Ordering::SeqCst);
		}
	});
	rt::sleep(Duration::from_millis(120)).await;
	assert_eq!(ticks.load(Ordering::SeqCst), 10);
	ticker.await.expect("the ticker finishes");
}

// ---------------------------------------------------------------------------------------------
// The first traps of the engine on this target
// ---------------------------------------------------------------------------------------------

/// `DefaultLogicalClock::new` read `SystemTime` and `Instant`, which panic here, and every commit
/// timestamp read `Instant::elapsed`.
#[wasm_bindgen_test]
fn the_logical_clock_does_not_trap_and_is_strictly_monotonic() {
	let clock = DefaultLogicalClock::new();
	let wall = (js_sys::Date::now() as u64) * 1_000_000;
	let first = clock.now();
	// Within a minute of the wall clock, in nanoseconds.
	assert!(first.abs_diff(wall) < 60_000_000_000, "{first} is not near {wall}");

	let mut last = first;
	for _ in 0..20_000 {
		let next = clock.now();
		assert!(next > last, "{next} does not follow {last}");
		last = next;
	}
	assert!(clock.current_timestamp() >= last);
	assert!(!format!("{clock:?}").is_empty());
}

/// Every background error is recorded with an `Instant`, which panicked here.
#[wasm_bindgen_test]
fn recording_a_background_error_does_not_trap() {
	let handler = BackgroundErrorHandler::new();
	let severity = handler
		.set_error(Error::Other("a flush failed".into()), BackgroundErrorReason::MemtablaFlush);
	let recorded = handler.get_error().expect("the error is recorded");
	assert_eq!(recorded.severity, severity);
	assert!(recorded.timestamp.elapsed() < Duration::from_secs(60));
	assert_eq!(handler.error_count(), 1);
}

struct Counts(StallCounts);

impl WriteStallCountProvider for Counts {
	fn get_stall_counts(&self) -> StallCounts {
		self.0
	}
}

/// Pacing a write near a stall slept on a tokio timer, which panics here between the soft and the
/// hard limit of the memtables.
#[wasm_bindgen_test]
async fn pacing_a_write_near_a_stall_does_not_trap() {
	let provider = Arc::new(Counts(StallCounts {
		immutable_memtables: 3,
		l0_files: 0,
	}));
	let controller = WriteStallController::new(provider, StallThresholds::new(4, 12));
	assert!(controller.check().await.expect("not stalled").is_none());

	let provider = Arc::new(Counts(StallCounts {
		immutable_memtables: 0,
		l0_files: 10,
	}));
	let controller = WriteStallController::new(provider, StallThresholds::new(4, 12));
	assert!(controller.check().await.expect("not stalled").is_none());
}

#[wasm_bindgen_test]
fn the_default_options_are_made_and_valid() {
	let options = Options::default();
	options.validate().expect("the defaults are valid");
	let _ = TreeBuilder::new();
}

/// A 2 GiB memtable is one allocation that a 32-bit address space cannot make, and a failed
/// allocation aborts the instance: `validate` refuses it, on the real target and not on a mock.
#[wasm_bindgen_test]
fn validate_refuses_a_memtable_of_2_gib_on_this_target() {
	assert_eq!(usize::BITS, 32);
	let refused = Options::default().with_max_memtable_size(2 << 30).validate();
	assert!(matches!(refused, Err(Error::InvalidArgument(_))), "{refused:?}");
	Options::default()
		.with_max_memtable_size(16 << 20)
		.validate()
		.expect("a browser-sized memtable is valid");
}

/// The hardware profile asks the OS for the number of cores, which `std` cannot say here.
#[wasm_bindgen_test]
fn auto_tuning_does_not_trap_and_stays_small() {
	let profile = crate::autotune::HardwareProfile::detect();
	assert_eq!(profile.cpu_cores, 1);
	let tuned = crate::autotune::auto_tune_options(Options::default());
	assert!(tuned.max_memtable_size <= 64 << 20, "{}", tuned.max_memtable_size);
	tuned.validate().expect("the tuned options are valid");
}

/// The file system is still `std::fs`, which a browser does not have, so a tree cannot be built.
/// What matters is that the build says so: it returns an error, and the instance is not aborted
/// by the clock, a thread, a timer or a spawner on the way.
#[wasm_bindgen_test]
fn building_a_tree_fails_cleanly_and_does_not_trap() {
	let built = TreeBuilder::new().with_path("/surrealkv-wasm-smoke".into()).build();
	assert!(built.is_err(), "std::fs cannot work in a browser");
	// The instance is alive: the shim still works.
	assert!(rt::Instant::now().elapsed() < Duration::from_secs(60));
}

// ---------------------------------------------------------------------------------------------
// Memory
// ---------------------------------------------------------------------------------------------

/// The size of the instance's linear memory, in 64 KiB pages. It never shrinks, so growth is leak.
fn memory_pages() -> u64 {
	let memory: js_sys::WebAssembly::Memory = wasm_bindgen::memory().unchecked_into();
	let buffer: js_sys::ArrayBuffer = memory.buffer().unchecked_into();
	u64::from(buffer.byte_length()) / 65536
}

/// A timer that is cancelled must not leave anything behind. It once did: the promise behind a
/// cleared `setTimeout` never settles, and the callbacks that `JsFuture` hung on it were never
/// freed, about 90 bytes for each timer that lost a `timeout` race or was dropped asleep.
#[wasm_bindgen_test]
async fn a_cancelled_timer_does_not_leak_memory() {
	use std::future::Future;
	use std::task::{Context, Poll, Waker};

	async fn cancel_some(rounds: usize) {
		let notify = tokio::sync::Notify::new();
		let mut context = Context::from_waker(Waker::noop());
		for round in 0..rounds {
			// A timeout that is polled (so its timer exists) and then dropped, as when the
			// notification arrives first, and a bare sleep that is dropped asleep.
			let mut waiting = Box::pin(rt::timeout(Duration::from_secs(3600), notify.notified()));
			assert!(waiting.as_mut().poll(&mut context).is_pending(), "round {round}");
			drop(waiting);
			let mut asleep = Box::pin(rt::sleep(Duration::from_secs(3600)));
			assert!(matches!(asleep.as_mut().poll(&mut context), Poll::Pending));
			drop(asleep);
			// Let the microtasks that settle the dropped timers run, so that the memory they
			// hold while they wait is not counted as growth.
			if round % 100 == 99 {
				rt::relax().await;
			}
		}
	}

	// Warm up, so that one-time allocations (the executor's queues, the allocator's arenas) are
	// not counted.
	cancel_some(1_000).await;
	rt::relax().await;
	let before = memory_pages();
	cancel_some(50_000).await;
	rt::relax().await;
	let grown = memory_pages() - before;
	// A leak of 90 bytes a timer is 100,000 timers, or about 140 pages.
	assert!(grown <= 8, "100,000 cancelled timers grew the memory by {grown} pages");
}

// ---------------------------------------------------------------------------------------------
// The file system seam
//
// A browser has no `std::fs`, so `Options::default()` starts from a file system that refuses
// everything, and a tree can only be built over one that was mounted. Nothing mounts one yet; what
// is held here is that the refusals are typed errors and not traps, and that the types that the
// pool will use work in a browser.
// ---------------------------------------------------------------------------------------------

/// The default is the file system with nothing mounted, in the options and in the log's context,
/// and it refuses every operation with `Unsupported` and the text that says what to do.
#[wasm_bindgen_test]
fn the_default_file_system_has_nothing_mounted_and_refuses_everything() {
	use crate::vfs::{self, FsKind};

	let options = Options::default();
	assert_eq!(options.fs.kind(), FsKind::Unavailable);
	assert_eq!(vfs::default_fs().kind(), FsKind::Unavailable);
	assert_eq!(crate::wal::WalContext::default().fs.kind(), FsKind::Unavailable);
	// The same options are valid: it is building a tree that says there is no file system.
	options.validate().expect("the default options are valid");
	vfs::unavailable::assert_refuses_everything(&*options.fs, "no file system is mounted");
}

/// Building a tree without a mounted file system is a typed error that names what to do, from
/// both entry points, before anything is read.
#[wasm_bindgen_test]
async fn building_without_a_mounted_file_system_is_a_typed_error() {
	let built = TreeBuilder::new().with_path("/surrealkv-wasm-fs".into()).build_async().await;
	match built {
		Err(Error::Io(e)) => {
			assert_eq!(e.kind(), std::io::ErrorKind::Unsupported);
			assert!(e.to_string().contains("no file system is mounted"), "{e}");
		}
		Err(other) => panic!("a typed refusal was expected, got {other:?}"),
		Ok(_) => panic!("a tree was built over no file system"),
	}
	let with_options = TreeBuilder::new().build_with_options_async().await;
	assert!(matches!(with_options, Err(Error::Io(_))));

	// The sync entry points refuse in the same way, before the first `exists` can answer `false`.
	for built in [
		TreeBuilder::new().with_path("/surrealkv-wasm-fs".into()).build().map(|_| ()),
		TreeBuilder::new().build_with_options().map(|_| ()),
	] {
		match built {
			Err(Error::Io(e)) => {
				assert_eq!(e.kind(), std::io::ErrorKind::Unsupported);
				assert!(e.to_string().contains("no file system is mounted"), "{e}");
			}
			other => panic!("a typed refusal was expected, got {other:?}"),
		}
	}

	// The in-memory file system is a pool, and the engine runs over it in the worker: a database is
	// opened, committed to, closed and opened again, on the only thread there is.
	let memory = crate::FileSystem::memory();
	let tree = TreeBuilder::new()
		.with_path("/surrealkv-wasm-memory".into())
		.with_file_system(memory.clone())
		.build_async()
		.await
		.expect("the engine builds over the memory file system");
	for i in 0..20u32 {
		let mut txn = tree.begin().unwrap();
		txn.set(i.to_be_bytes().to_vec(), b"value".to_vec()).unwrap();
		txn.commit().await.unwrap();
	}
	tree.close().await.unwrap();
	drop(tree);
	let tree = TreeBuilder::new()
		.with_path("/surrealkv-wasm-memory".into())
		.with_file_system(memory)
		.build_async()
		.await
		.expect("and opens again");
	let txn = tree.begin().unwrap();
	for i in 0..20u32 {
		assert_eq!(txn.get(i.to_be_bytes().to_vec()).unwrap(), Some(b"value".to_vec()));
	}
	drop(txn);
	tree.close().await.unwrap();
	// Still alive.
	assert!(rt::Instant::now().elapsed() < Duration::from_secs(60));
}

/// Dropping the last `Tree` closes the database in the background, although a transaction that
/// outlives it holds the core: the lock is given back, and the database opens again in the worker.
#[wasm_bindgen_test]
async fn the_last_tree_dropped_closes_the_database_while_a_transaction_lives() {
	let memory = crate::FileSystem::memory();
	let build = || {
		TreeBuilder::new()
			.with_path("/surrealkv-wasm-drop".into())
			.with_file_system(memory.clone())
			.build_async()
	};
	let tree = build().await.expect("the database opens");
	let clone = tree.clone();
	let mut txn = tree.begin().unwrap();
	txn.set(b"k".to_vec(), b"v".to_vec()).unwrap();
	// One tree is left: nothing closes.
	drop(tree);
	rt::sleep(Duration::from_millis(50)).await;
	assert!(build().await.is_err(), "the database is still open");
	// The last one goes, with the transaction alive: the close runs in the background.
	drop(clone);
	let mut reopened = None;
	for _ in 0..200 {
		match build().await {
			Ok(tree) => {
				reopened = Some(tree);
				break;
			}
			Err(_) => rt::sleep(Duration::from_millis(10)).await,
		}
	}
	let reopened = reopened.expect("the lock was given back by the close of the last tree");
	assert!(txn.commit().await.is_err(), "the transaction that outlived it finds it shut");
	reopened.close().await.unwrap();
}

/// The lock that a pool hands out gives its entry back when it is dropped: on this target too,
/// where the lock of a native build is a no-op and had no `Drop`.
#[wasm_bindgen_test]
fn a_registered_lock_is_one_per_directory_and_is_given_back() {
	crate::lockfile::check_registered_locks();
}

/// The typed error of a pool that has no free file: its payload survives the conversions, and it
/// is a soft error for the background tasks, in a browser as it is natively.
#[wasm_bindgen_test]
fn an_exhausted_pool_is_a_typed_soft_error() {
	use crate::error::ErrorSeverity;
	use crate::vfs;

	assert!(matches!(Error::from(vfs::pool_exhausted()), Error::FilePoolExhausted));
	assert_eq!(crate::wal::Error::from(vfs::pool_exhausted()), crate::wal::Error::PoolExhausted);
	assert!(matches!(Error::from(crate::wal::Error::PoolExhausted), Error::FilePoolExhausted));
	assert!(matches!(Error::from(std::io::Error::other("full")), Error::Io(_)));
	for reason in [
		BackgroundErrorReason::MemtablaFlush,
		BackgroundErrorReason::Compaction,
		BackgroundErrorReason::ManifestWrite,
	] {
		assert_eq!(
			BackgroundErrorHandler::classify_error(&Error::FilePoolExhausted, reason),
			ErrorSeverity::SoftError
		);
	}
	assert_eq!(
		BackgroundErrorHandler::classify_error(
			&Error::FilePoolExhausted,
			BackgroundErrorReason::CommitGroup
		),
		ErrorSeverity::Unrecoverable
	);
	let handler = BackgroundErrorHandler::new();
	handler.set_error(Error::FilePoolExhausted, BackgroundErrorReason::MemtablaFlush);
	assert!(!handler.is_db_stopped());
}

// ---------------------------------------------------------------------------------------------
// The pool over memory, which is the pool of the browser with a different backend under it.
// ---------------------------------------------------------------------------------------------

/// The pool's own code runs on this target: the whole contract of the seam, and what only a pool
/// promises (the handles of a file are one file, a reader keeps what it had), over memory, from a
/// plain function with no runtime.
#[wasm_bindgen_test]
fn the_pool_over_memory_honours_the_contract_in_a_browser() {
	use std::path::Path;

	use crate::vfs::{contract, Fs, MemRaw, PoolConfig, PoolFs};

	let (fs, report) = PoolFs::<MemRaw>::mount_sync(MemRaw::new(), PoolConfig::memory())
		.expect("a pool over memory mounts without a runtime");
	assert!(report.orphans_kept.is_empty(), "{report:?}");
	let root = Path::new("/root");
	fs.create_dir_all(root).unwrap();
	contract::run_all(&*fs, root);
	let pool = Path::new("/pool");
	fs.create_dir_all(pool).unwrap();
	contract::pool_semantics(&*fs, pool);
}

/// `FileSystem::memory()` through the crate's file system: creates, appends, syncs, renames and
/// lists, then a crash that loses what was not synced, a remount of what is left, and a close that
/// releases every file.
#[wasm_bindgen_test]
async fn the_memory_file_system_survives_a_crash_and_remounts_in_a_browser() {
	use std::path::Path;

	use crate::vfs::{CrashMode, CreateOpts};
	use crate::FileSystem;

	let fs = FileSystem::memory();
	let inner = &fs.0;
	let dir = Path::new("/db/wal");
	inner.create_dir_all(dir).unwrap();

	let synced = inner.create_new(&dir.join("00000001.wal"), CreateOpts::default()).unwrap();
	synced.append(b"acknowledged").unwrap();
	synced.sync().unwrap();
	let renamed = inner.create_truncate(&dir.join("tmp"), CreateOpts::default()).unwrap();
	renamed.append(b"renamed bytes").unwrap();
	renamed.sync().unwrap();
	inner.rename(&dir.join("tmp"), &dir.join("MANIFEST")).unwrap();
	inner.sync_dir(dir).unwrap();
	let lost = inner.create_new(&dir.join("00000002.wal"), CreateOpts::default()).unwrap();
	lost.append(b"never synced").unwrap();
	assert_eq!(lost.len().unwrap(), 12, "an unsynced write is visible before the crash");

	assert_eq!(fs.list_for_test("/db/wal"), ["00000001.wal", "00000002.wal", "MANIFEST"]);
	assert_eq!(fs.file_len_for_test("/db/wal/00000001.wal"), Some(12));
	assert_eq!(fs.file_len_for_test("/db/wal/MANIFEST"), Some(13));
	assert!(fs.list_for_test("/not/there").is_empty());

	// A crash of the state above, which is not changed by it.
	let after = fs.simulate_crash(CrashMode::LoseUnsynced).expect("the memory file system crashes");
	assert_eq!(after.0.read(&dir.join("00000001.wal")).unwrap(), b"acknowledged");
	assert_eq!(after.0.read(&dir.join("MANIFEST")).unwrap(), b"renamed bytes");
	assert!(after.list_for_test("/db/wal").contains(&"MANIFEST".to_string()));
	assert!(
		after.file_len_for_test("/db/wal/00000002.wal").unwrap_or(0) == 0,
		"what was never synced did not survive"
	);
	assert!(after.mount_report().is_some());
	assert_eq!(fs.file_len_for_test("/db/wal/00000002.wal"), Some(12), "the original is untouched");

	// A torn tail: a file cut back below what it held, without a flush.
	fs.raw_truncate_for_test("/db/wal/00000001.wal", 5).unwrap();
	assert_eq!(fs.file_len_for_test("/db/wal/00000001.wal"), Some(5));

	// Close releases every file, and a file used after it fails.
	after.close().await.expect("a pool closes");
	assert!(after.0.read(&dir.join("MANIFEST")).is_err());
	fs.abandon();
	assert!(FileSystem::memory().simulate_crash(CrashMode::ZeroTail).is_ok());
}
