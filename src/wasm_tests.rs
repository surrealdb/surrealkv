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
