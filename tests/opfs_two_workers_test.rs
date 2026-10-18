//! The engine in several workers of one headless Chrome, as it runs in several tabs of a browser.
//! Run it, from the repository root, with
//!
//! ```text
//! export CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang
//! export AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
//! WASM_BINDGEN_TEST_TIMEOUT=600 WASM_BINDGEN_USE_DEDICATED_WORKER=1 \
//!     wasm-pack test --headless --chrome -- --test opfs_two_workers_test -- --nocapture
//! ```
//!
//! (`--nocapture` shows the lines that start with `TERMINATE_WAIT`, `KILL`, `HAND_OVER`,
//! `FORCED_END` and `SUMMARY`, which a passing test does not print otherwise.) Every other browser
//! test runs the engine in one worker. Here the test itself runs in a dedicated worker, the
//! *coordinator*, which starts other dedicated workers that run the same wasm module (see
//! `opfs_common/workers.rs` for how), sends them commands, and ends them. The coordinator does not
//! run the engine, only looks at the origin private file system.
//!
//! What is shown:
//!
//! 1. Exclusivity. A second worker cannot mount a database that a first one holds: it fails with
//!    the typed lock error, after the wait that it was given and not much later, it leaves no file
//!    behind and cannot remove the database either, and the first worker goes on, unharmed.
//! 2. A hand-over after a clean close. The next worker mounts at once and finds every key, six
//!    times in a row, with and without the value log.
//! 3. A hand-over to a worker that was already waiting for the database: it mounts after the first
//!    worker closed and not before, and before its deadline.
//! 4. A kill. A worker commits in a loop, and the coordinator asks the browser to end it at a point
//!    that it chooses (after a number of commits, at a pseudo-random moment, among flushes and
//!    compactions, in the middle of `Tree::close`); the next worker mounts, waiting as long as the
//!    browser needs to release the files, and finds a database that holds the commits of the first
//!    worker in their order and none after a gap, each commit whole, and, if they were
//!    `Durability::Immediate`, every one that the worker had acknowledged, which the worker wrote
//!    to a file of its own as it went, since the browser drops the messages of an ended worker. The
//!    next worker commits and closes, and a third one reads everything.
//! 5. Two databases at once, one in each of two workers, worked on at the same time: neither sees a
//!    key of the other.
//! 6. The move out of IndexedDB, killed at several moments, then called again by another worker:
//!    the database equals the source, the source is as it was and the marker is gone.
//!
//! # How a worker ends, and what that does not show
//!
//! `Worker.terminate()` does not stop a worker at once. Chrome ends it when the task that it is
//! running is over, and, if the task is still running about two seconds after the request, stops it
//! there, wherever the code is (see the comment before section 4 and `forced_after_ms`). So a
//! request ends a worker either *between* two tasks, which for the engine is wherever it waits for
//! the browser (to open a file, for a timer), or in the *middle* of one, which the tests ask for
//! with a loop that never returns to the event loop and with a close that follows a busy wait. A
//! call on a synchronous access handle is not known to be interrupted half way, so what is reached
//! is the points between two calls of the engine, and not a write that is torn inside one call. The
//! browser then closes the handles of the worker, when it gets to it: the next worker mounts with a
//! wait of [`RELEASE_WAIT_MS`], every wait is printed, and the test fails only if that one passes.
//!
//! What is not covered: a crash of the browser or of its renderer process, the loss of the
//! machine's power, and what the operating system keeps of a write that it has not flushed (see the
//! section on durability in the README). `terminate()` leaves everything that the worker had
//! written in the file system of the browser.
//!
//! The kill tests run [`ROUNDS`] rounds each, so that the target stays within a few minutes: a
//! round that ends a worker in the middle of a task takes two seconds longer than the others.
//! `SKV_TWO_WORKER_ROUNDS=20` at compile time (it is read with `option_env!`) makes a soak run, and
//! `SKV_TWO_WORKER_SEED` changes the seed that the moments of the kills come from; both are
//! printed.

#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

#[path = "opfs_common/idb.rs"]
mod idb;
mod opfs_common;
#[path = "opfs_common/workers.rs"]
mod workers;

use opfs_common::{destroy_when_released, list_directory, sleep_ms, unique_name, watchdog, Rng};
use wasm_bindgen_test::*;
use workers::{begin_test, end_test, Child, CommitSpec, Db, Wait, Yielding};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// How long the worker that follows a killed one waits for the database before the test fails. The
/// browser ends a worker when its task is over and stops it about two seconds later at the latest,
/// and releases its files after that.
const RELEASE_WAIT_MS: u64 = 120_000;

/// How long the removal of a database that no worker has mounted may wait for the browser.
const REMOVE_WAIT_MS: f64 = 120_000.0;

/// Kill rounds of each of the kill tests.
const ROUNDS: usize = number(option_env!("SKV_TWO_WORKER_ROUNDS"), 4);

/// Where the moments of the kills come from.
const SEED: u64 = number(option_env!("SKV_TWO_WORKER_SEED"), 0x5EED_0F7B) as u64;

const fn number(text: Option<&str>, default: usize) -> usize {
	let Some(text) = text else {
		return default;
	};
	let bytes = text.as_bytes();
	assert!(!bytes.is_empty(), "an empty number in the environment of the build");
	let (mut n, mut i) = (0usize, 0usize);
	while i < bytes.len() {
		assert!(
			bytes[i].is_ascii_digit(),
			"the number in the environment of the build is not decimal"
		);
		n = n * 10 + (bytes[i] - b'0') as usize;
		i += 1;
	}
	n
}

fn now() -> f64 {
	js_sys::Date::now()
}

/// Removes the database and checks that no worker is left.
async fn finish(test: &str, db: &Db) {
	destroy_when_released(&db.name, REMOVE_WAIT_MS).await;
	end_test(test);
}

// ---------------------------------------------------------------------------------------------
// 1. Exclusivity
// ---------------------------------------------------------------------------------------------

async fn exclusivity(test: &str, vlog: bool) {
	begin_test(test);
	let db = Db::new("two-excl", vlog);
	let a = Child::spawn("A").await;
	a.mount_and_open(&db).await;
	a.commit(&CommitSpec::new(0, 200, true)).await.unwrap("A commits 200 keys");

	let before =
		list_directory(&db.name).await.expect("the directory of the database that A holds");
	assert!(before.iter().any(|name| name == "LOCK"), "no lock file in {before:?}");

	let b = Child::spawn("B").await;
	for wait in [0u64, 400, 1_500] {
		let sent = now();
		let attempt = b.mount(&db, Wait::Ms(wait)).await;
		let round_trip = now() - sent;
		assert!(
			attempt.is_locked(),
			"B mounted a database that A holds, or failed otherwise: {attempt:?}"
		);
		let waited = attempt.number("ms");
		// It tries until the wait is over, and gives up soon after.
		assert!(waited >= wait as i64 - 2, "B gave up after {waited} ms of a wait of {wait} ms");
		assert!(waited <= wait as i64 + 2_000, "B waited {waited} ms for a wait of {wait} ms");
		console_log!("EXCLUSIVITY wait={wait} gave_up_after={waited} round_trip={round_trip:.0}");
	}
	// Nor can B remove the database that A holds.
	let removal = b.call(&format!("destroy db={}", db.name), "B removes the database").await;
	assert!(removal.is_locked(), "B removed or failed to remove the database of A: {removal:?}");
	let after =
		list_directory(&db.name).await.expect("the directory of the database after B's attempts");
	assert_eq!(before, after, "B's attempts changed the files of the pool");

	// A did not notice.
	a.commit(&CommitSpec::new(200, 100, true)).await.unwrap("A commits 100 more");
	a.verify(0, 300).await.unwrap("A reads all 300");
	let probe = a.probe().await;
	assert!(probe.is_a_prefix() && probe.prefix == 300, "{probe:?}");

	a.close().await.unwrap("A closes");
	// And once A has closed, B gets it.
	b.mount(&db, Wait::Ms(0)).await.unwrap("B mounts after A closed");
	b.open().await.unwrap("B opens");
	b.verify(0, 300).await.unwrap("B reads all 300");
	b.exit().await.unwrap("B exits");
	a.exit().await;
	finish(test, &db).await;
}

#[wasm_bindgen_test]
async fn a_second_worker_cannot_mount_a_database_that_a_first_holds_with_the_value_log() {
	exclusivity("exclusivity_with_the_value_log", true).await;
}

#[wasm_bindgen_test]
async fn a_second_worker_cannot_mount_a_database_that_a_first_holds_without_the_value_log() {
	exclusivity("exclusivity_without_the_value_log", false).await;
}

// ---------------------------------------------------------------------------------------------
// 2. Hand-over after a clean close
// ---------------------------------------------------------------------------------------------

async fn hand_overs(test: &str, vlog: bool) {
	const HAND_OVERS: u64 = 6;
	const PER_WORKER: u64 = 50;
	begin_test(test);
	let db = Db::new("two-chain", vlog);
	let mut total = 0u64;
	for round in 0..HAND_OVERS {
		let worker = Child::spawn(&format!("W{round}")).await;
		let mounted = worker.mount(&db, Wait::Default).await.unwrap("mount after a clean close");
		// At once: the lock was released by the close, and nothing waits for the browser.
		assert!(
			mounted.number("ms") < 4_000,
			"the mount after a clean close took {:?} ms",
			mounted.number("ms")
		);
		let opened = worker.open().await.unwrap("open");
		console_log!(
			"HAND_OVER round={round} mount_ms={} open_ms={} replayed={} tables={}",
			mounted.number("ms"),
			opened.number("ms"),
			opened.number("replayed"),
			opened.number("tables")
		);
		// Everything the earlier workers wrote, and nothing else.
		let probe = worker.probe().await;
		assert!(
			probe.is_a_prefix() && probe.prefix == total,
			"round {round}: {probe:?}, expected {total}"
		);
		if total > 0 {
			worker.verify(0, total).await.unwrap("read what the earlier workers wrote");
		}
		// The commits alternate between durable and not: a clean close covers both.
		let spec = CommitSpec::new(total, PER_WORKER, round % 2 == 0).per(10);
		worker.commit(&spec).await.unwrap("commit");
		total += PER_WORKER;
		worker.close().await.unwrap("close");
		worker.exit().await.unwrap("exit");
	}
	let last = Child::spawn("A2").await;
	last.mount_and_open(&db).await;
	last.verify(0, total).await.unwrap("the last worker reads everything");
	let probe = last.probe().await;
	assert!(probe.is_a_prefix() && probe.prefix == total, "{probe:?}, expected {total}");
	last.close().await.unwrap("close");
	last.exit().await.unwrap("exit");
	finish(test, &db).await;
}

#[wasm_bindgen_test]
async fn six_hand_overs_after_a_clean_close_lose_nothing_with_the_value_log() {
	hand_overs("hand_overs_with_the_value_log", true).await;
}

#[wasm_bindgen_test]
async fn six_hand_overs_after_a_clean_close_lose_nothing_without_the_value_log() {
	hand_overs("hand_overs_without_the_value_log", false).await;
}

// ---------------------------------------------------------------------------------------------
// 3. A taker that waits
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn a_worker_that_waits_for_the_database_gets_it_when_the_holder_closes() {
	const HOLD_MS: f64 = 2_000.0;
	const DEADLINE_MS: u64 = 60_000;
	let test = "a_worker_that_waits";
	begin_test(test);
	let db = Db::new("two-wait", true);
	let a = Child::spawn("A").await;
	a.mount_and_open(&db).await;
	a.commit(&CommitSpec::new(0, 100, true).per(10)).await.unwrap("A commits");

	let b = Child::spawn("B").await;
	let mount = b.send_mount(&db, Wait::Ms(DEADLINE_MS));
	b.wait_until("B to begin its mount", 30_000.0, |b| !b.events("mounting").is_empty()).await;
	sleep_ms(HOLD_MS).await;
	assert!(b.events("mounted").is_empty(), "B mounted a database that A still holds");

	let close_sent = now();
	a.close().await.unwrap("A closes");
	let mounted =
		b.wait(mount, DEADLINE_MS as f64, "B's mount after A closed").await.unwrap("B mounts");
	// B could not have the database before A was asked to let it go, it waited for that, and it
	// got it well before its deadline.
	assert!(mounted.seen_at >= close_sent, "B's mount was answered before A was asked to close");
	assert!(
		mounted.number("ms") as f64 >= HOLD_MS - 500.0,
		"B mounted after {} ms",
		mounted.number("ms")
	);
	assert!(
		mounted.number("ms") < DEADLINE_MS as i64 / 2,
		"B mounted after {} ms",
		mounted.number("ms")
	);
	console_log!(
		"HAND_OVER waiting taker: mounted {:.0} ms after A was asked to close, {} ms after it began",
		mounted.seen_at - close_sent,
		mounted.number("ms")
	);

	b.open().await.unwrap("B opens");
	b.verify(0, 100).await.unwrap("B reads what A wrote");
	b.commit(&CommitSpec::new(100, 50, true).per(10)).await.unwrap("B commits");
	b.close().await.unwrap("B closes");
	let c = Child::spawn("C").await;
	c.mount_and_open(&db).await;
	c.verify(0, 150).await.unwrap("C reads what A and B wrote");
	c.close().await.unwrap("C closes");
	for child in [&a, &b, &c] {
		child.exit().await.unwrap("exit");
	}
	finish(test, &db).await;
}

// ---------------------------------------------------------------------------------------------
// 4. A kill
// ---------------------------------------------------------------------------------------------
//
// What `Worker.terminate()` does, as measured in Chrome (see `forced_after_ms`): the worker is not
// stopped at once. It is ended when the task that it is running is over, and if that task is still
// running about two seconds after the request, it is stopped there, wherever the code is. (What
// closing a tab does to its workers was not measured.) The two are different tests of the engine:
//
// * A *graceful* end comes between two tasks of the worker, that is, wherever the engine waits for
//   the browser (to open a file, for a timer) or the loop that drives it returns to the event loop.
//   A flush, a compaction or a close that never waits for the browser is one task, and a request
//   that arrives in the middle of it ends the worker after it.
// * A *forced* end comes in the middle of a task, between two calls that the engine makes (a call
//   on a synchronous access handle is not interrupted, as far as is known), which is where a crash
//   tears a sequence of writes. A commit loop that never returns to the event loop
//   ([`Yielding::Tasks`]) and a close that follows a long busy wait are such tasks, and the worker
//   that runs them is stopped about two seconds after the request.

/// Commits of the loop of the worker that is killed: more than it can make before it is ended.
const LOOP_COMMITS: u64 = 1_000_000;

/// Indices that the next worker commits after a kill, and the one after it reads.
const AFTER_KILL: u64 = 40;

/// What a round of a kill test saw.
struct Seen {
	/// Indices the first worker had acknowledged when it was gone, from the file it wrote them to.
	acked: u64,
	/// The length of the prefix that the next worker found.
	present: u64,
	/// How long the next worker waited for the database after the first one was asked to end.
	waited_ms: f64,
}

thread_local! {
	static FORCED_AFTER_MS: std::cell::Cell<Option<f64>> = const { std::cell::Cell::new(None) };
}

/// How long after the request Chrome ends a worker that does not return to its event loop: a worker
/// that holds a database is asked to end in the middle of a long task, and the time until another
/// can mount the database is measured (once, and kept). It is about two seconds, which is the
/// delay that the browser lets a worker finish its task for.
async fn forced_after_ms() -> f64 {
	if let Some(known) = FORCED_AFTER_MS.with(|known| known.get()) {
		return known;
	}
	let db = Db::new("two-spin", true);
	let a = Child::spawn("A").await;
	a.mount_and_open(&db).await;
	a.send_spin(30_000);
	a.wait_until("the worker to begin its long task", 60_000.0, |a| {
		!a.events("spinning").is_empty()
	})
	.await;
	a.terminate();
	let ended = a.terminated_at().expect("the worker was asked to end");
	let b = Child::spawn("B").await;
	let mounted = b.mount(&db, Wait::Ms(RELEASE_WAIT_MS)).await;
	let waited = now() - ended;
	assert!(mounted.ok, "the files of a worker in a long task were not released in {waited:.0} ms");
	b.close().await.unwrap("close");
	b.exit().await;
	destroy_when_released(&db.name, REMOVE_WAIT_MS).await;
	console_log!("FORCED_END after_ms={waited:.0}");
	FORCED_AFTER_MS.with(|known| known.set(Some(waited)));
	waited
}

/// When the coordinator asks the worker to end.
#[derive(Clone, Copy, Debug)]
enum Kill {
	/// As soon as the acknowledgement of this many indices arrives.
	Acks(u64),
	/// This long after the first acknowledgement.
	Delay(u64),
	/// This long after the worker has been seen to flush a memtable to a table and to remove
	/// tables by a compaction.
	Compaction(u64),
}

/// What a worker wrote to a file of its own at the root of the origin private file system, which
/// tells what it had done when it was stopped, whether or not its messages got through (the browser
/// drops them from the request to end the worker on). The file is open in the worker until the
/// browser has released its handles, so this waits for that, and it removes the file.
async fn take_side_file(name: &str) -> Vec<u8> {
	let root = opfs_common::root().await;
	let started = now();
	loop {
		match surrealkv::storage::opfs::open_opfs_sync_file(&root, name, false).await {
			Ok(file) => {
				let bytes = opfs_common::read_all(&file);
				file.close();
				opfs_common::remove_and_verify(&root, name).await;
				return bytes;
			}
			Err(e) => {
				let text = format!("{e:?}");
				assert!(
					now() - started < 30_000.0 && !text.contains("NotFound"),
					"the file {name} could not be opened: {text}"
				);
				sleep_ms(20.0).await;
			}
		}
	}
}

/// The number of indices that a worker had committed when it was stopped, from its ack file.
async fn acked_in_file(name: &str) -> u64 {
	let bytes = take_side_file(name).await;
	match <[u8; 8]>::try_from(bytes.as_slice()) {
		Ok(eight) => u64::from_le_bytes(eight),
		Err(_) => {
			assert!(bytes.is_empty(), "the ack file {name} holds {} bytes", bytes.len());
			0
		}
	}
}

/// The tables that a worker reported: the most it had at once, and whether the number ever fell,
/// which only a compaction does.
fn table_history(worker: &Child) -> (u64, bool) {
	let counts: Vec<u64> = worker.events("sst").iter().filter_map(|e| e.arg.parse().ok()).collect();
	let most = counts.iter().copied().max().unwrap_or(0);
	let fell = counts.windows(2).any(|pair| pair[1] < pair[0]);
	(most, fell)
}

/// One round: the first worker commits one index (or `per`) at a time and is asked to end, in one
/// task if `busy` (so that the browser stops it in the middle of it) and between tasks if not; the
/// second finds the database and has to make sense of it; the third reads what is there at the end.
async fn kill_round(
	label: &str,
	round: usize,
	db: Db,
	durable: bool,
	per: u64,
	kill: Kill,
	busy: bool,
) -> Seen {
	let a = Child::spawn("A").await;
	a.mount_and_open(&db).await;
	let flush_heavy = db.mem != 0;
	let ack_file = format!("{}-acks", db.name);
	let spec = CommitSpec::new(0, LOOP_COMMITS, durable)
		.per(per)
		.ack()
		.ack_file(&ack_file)
		.yielding(if busy {
			Yielding::Tasks
		} else {
			Yielding::EventLoop
		})
		.sst_every(if flush_heavy {
			5
		} else {
			0
		});
	// Never waited for: the worker is ended in the middle of it.
	a.send_commit(&spec);
	match kill {
		Kill::Acks(n) => {
			a.terminate_at_ack(n);
			a.wait_until("the acknowledgements to reach the kill point", 120_000.0, |a| {
				a.is_terminated()
			})
			.await;
		}
		Kill::Delay(ms) => {
			a.wait_until("the first acknowledgement", 120_000.0, |a| a.acked() >= 1).await;
			sleep_ms(ms as f64).await;
			a.terminate();
		}
		Kill::Compaction(ms) => {
			a.wait_until("a flush and a compaction", 300_000.0, |a| {
				let (most, fell) = table_history(a);
				most >= 1 && fell
			})
			.await;
			sleep_ms(ms as f64).await;
			a.terminate();
		}
	}
	let asked = a.terminated_at().expect("the worker was asked to end");
	let acked_at_request = a.acked();
	let (most_tables, tables_fell) = table_history(&a);

	// The next worker waits as long as the browser needs to release the files.
	let b = Child::spawn("B").await;
	let mounted = b.mount(&db, Wait::Ms(RELEASE_WAIT_MS)).await;
	let waited_ms = now() - asked;
	assert!(
		mounted.ok,
		"{label} round {round}: B could not mount {waited_ms:.0} ms after A was asked to end ({}): {}",
		mounted.kind, mounted.message
	);
	// What A had acknowledged when it was gone. A worker that is stopped in the middle of its task
	// goes on for a time after the request, and its messages are dropped from the request on, so
	// what it wrote to its file is what it knew.
	let acked = acked_in_file(&ack_file).await;
	console_log!(
		"TERMINATE_WAIT test={label} round={round} ms={waited_ms:.0} busy={busy} \
		 acked_messages_at_request={acked_at_request} acked_in_file={acked}"
	);
	let opened = b.open().await.unwrap("B opens the database of the worker that was ended");
	let probe = b.probe().await;
	assert!(
		probe.is_a_prefix(),
		"{label} round {round} ({kill:?}): what B found is not a prefix of A's commits: {probe:?}"
	);
	// Each commit is whole: the indices of one transaction are all there or none is.
	assert_eq!(
		probe.prefix % per,
		0,
		"{label} round {round}: a commit of {per} indices is torn: {probe:?}"
	);
	if durable {
		assert!(
			probe.prefix >= acked,
			"{label} round {round} ({kill:?}): A was told {acked} indices were durable and B found {}",
			probe.prefix
		);
	}
	if probe.prefix > 0 {
		b.verify(0, probe.prefix).await.unwrap("B reads what it found with get");
	}
	// With no table to read, what B found came from the replay of the log.
	if probe.prefix > 0 && opened.number("tables") == 0 {
		assert!(
			opened.number("replayed") > 0,
			"{label} round {round}: the data of a database with no table was not replayed"
		);
	}
	console_log!(
		"KILL test={label} round={round} kill={kill:?} busy={busy} durable={durable} acked_in_file={acked} \
		 present={} files_at_mount={} cut_back={} replayed={} segments={} trimmed={} dropped={} \
		 tables_at_open={} most_tables_seen={most_tables} a_compaction_seen={tables_fell}",
		probe.prefix,
		mounted.number("files"),
		mounted.number("cut_back"),
		opened.number("replayed"),
		opened.number("segments"),
		opened.number("trimmed"),
		opened.number("dropped"),
		opened.number("tables")
	);

	// B goes on, and a third worker reads everything.
	let next = probe.prefix;
	b.commit(&CommitSpec::new(next, AFTER_KILL, true).per(per))
		.await
		.unwrap("B commits after the kill");
	b.close().await.unwrap("B closes");
	b.exit().await.unwrap("B exits");
	let c = Child::spawn("C").await;
	c.mount_and_open(&db).await;
	c.verify(0, next + AFTER_KILL).await.unwrap("C reads what A and B wrote");
	let probe = c.probe().await;
	assert!(
		probe.is_a_prefix() && probe.prefix == next + AFTER_KILL,
		"{label} round {round}: C found {probe:?}, expected {}",
		next + AFTER_KILL
	);
	c.close().await.unwrap("C closes");
	c.exit().await.unwrap("C exits");
	Seen {
		acked,
		present: next,
		waited_ms,
	}
}

/// A moment below `below_ms` for a kill: it follows from the seed, the test and the round, so that
/// a run can be repeated and the tests do not all kill at the same moments.
fn moment(test: &str, round: usize, below_ms: u64) -> u64 {
	let salt = test.bytes().fold(0u64, |h, b| h.wrapping_mul(31).wrapping_add(u64::from(b)));
	Rng::new(SEED ^ salt ^ (round as u64).wrapping_mul(0x9E37_79B9)).below(below_ms)
}

/// Rounds of a kill test: the first asks the worker to end after a fixed number of acknowledged
/// commits, the others at a pseudo-random moment, and every third of them in the middle of a task.
async fn kill_test(test: &str, vlog: bool, durable: bool) {
	begin_test(test);
	console_log!("{test}: {ROUNDS} rounds, seed {SEED:#x}");
	let mut seen = Vec::new();
	for round in 0..ROUNDS {
		let db = Db::new("two-kill", vlog);
		let (kill, busy) = if round == 0 {
			(Kill::Acks(60), false)
		} else {
			(Kill::Delay(moment(test, round, 700)), round % 3 == 1)
		};
		seen.push(kill_round(test, round, db.clone(), durable, 1, kill, busy).await);
		destroy_when_released(&db.name, REMOVE_WAIT_MS).await;
	}
	summarise(test, &seen);
	end_test(test);
}

fn summarise(test: &str, seen: &[Seen]) {
	let waits: Vec<String> = seen.iter().map(|s| format!("{:.0}", s.waited_ms)).collect();
	let present: Vec<String> = seen.iter().map(|s| format!("{}/{}", s.present, s.acked)).collect();
	console_log!(
		"SUMMARY test={test} waits_ms=[{}] present/acked=[{}]",
		waits.join(","),
		present.join(",")
	);
}

#[wasm_bindgen_test]
async fn a_killed_worker_leaves_every_durable_commit_with_the_value_log() {
	kill_test("kill_durable_with_the_value_log", true, true).await;
}

#[wasm_bindgen_test]
async fn a_killed_worker_leaves_every_durable_commit_without_the_value_log() {
	kill_test("kill_durable_without_the_value_log", false, true).await;
}

#[wasm_bindgen_test]
async fn a_killed_worker_that_commits_eventually_leaves_a_prefix_with_the_value_log() {
	kill_test("kill_eventual_with_the_value_log", true, false).await;
}

#[wasm_bindgen_test]
async fn a_killed_worker_that_commits_eventually_leaves_a_prefix_without_the_value_log() {
	kill_test("kill_eventual_without_the_value_log", false, false).await;
}

/// The worker is ended among flushes and compactions: the memtable is small, so that the tables
/// and the log segments change all the time, and the commits go four indices at a time to fill it
/// faster. Every other round ends the worker in the middle of its task.
#[wasm_bindgen_test]
async fn a_worker_killed_among_flushes_and_compactions_leaves_every_durable_commit() {
	let test = "kill_among_flushes_and_compactions";
	begin_test(test);
	console_log!("{test}: {ROUNDS} rounds, seed {SEED:#x}");
	let mut seen = Vec::new();
	for round in 0..ROUNDS {
		// The value log is on in one round and off in the next.
		let db = Db::new("two-flush", round % 2 == 0).with_mem(128 * 1024);
		let kill = Kill::Compaction(moment(test, round, 300));
		seen.push(kill_round(test, round, db.clone(), true, 4, kill, round % 2 == 1).await);
		destroy_when_released(&db.name, REMOVE_WAIT_MS).await;
	}
	summarise(test, &seen);
	end_test(test);
}

/// What a worker had reached of its close when it was stopped, from the byte it wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CloseState {
	/// The close had not begun: the worker was still in its busy wait.
	Before,
	/// The close was running.
	Inside,
	/// The close had returned.
	After,
}

/// The worker is ended while `Tree::close` runs. Every commit was durable before the close began,
/// so all of them have to be there whatever the close had done by then. The memtable is large, so
/// that nothing is flushed before the close, which flushes it to a table: a worker that is stopped
/// early in the close leaves the log to be replayed, and one that is stopped late leaves a table
/// and no log.
///
/// The close is a task of its own, which a request to end the worker does not interrupt: the first
/// round asks for it and finds the database closed. The others begin the close after a busy wait in
/// the same task, so that the browser stops the worker about `forced` ms after the request, and the
/// busy waits are spread over a window of 230 ms that ends 20 ms before that. Where the stop falls
/// in the close is read from a file that the worker wrote its progress to (`CloseState`), since
/// its messages are dropped, and the test asserts that some rounds were stopped inside the close.
async fn kill_in_close(test: &str, vlog: bool) {
	const INDICES: u64 = 16_000;
	// The window of the busy waits, in ms before `forced`.
	const WINDOW_START_MS: f64 = 250.0;
	const WINDOW_END_MS: f64 = 20.0;
	let forced_rounds = ROUNDS * 2;
	begin_test(test);
	console_log!("{test}: {forced_rounds} rounds that wait for the browser, seed {SEED:#x}");
	let forced = forced_after_ms().await;
	let mut states = Vec::new();
	let mut waits = Vec::new();
	// Round 0 closes cleanly, and round 1 asks at the start of the close, which runs to its end.
	for round in 0..forced_rounds + 2 {
		let db = Db::new("two-close", vlog);
		let state_file = format!("{}-close", db.name);
		let a = Child::spawn("A").await;
		a.mount_and_open(&db).await;
		a.commit(&CommitSpec::new(0, INDICES, true).per(200)).await.unwrap("A commits");
		let asked = if round == 0 {
			let closed = a.close_tree().await.unwrap("A closes the tree");
			console_log!("KILL test={test} round=0 (not ended) close_ms={}", closed.number("ms"));
			a.close().await.unwrap("A closes");
			a.terminate();
			None
		} else {
			let busy_ms = if round == 1 {
				0
			} else {
				let i = (round - 2) as f64;
				let jitter = moment(test, round, 1000) as f64 / 1000.0;
				let f = (i + jitter) / forced_rounds as f64;
				(forced - WINDOW_END_MS - f * (WINDOW_START_MS - WINDOW_END_MS)).max(0.0) as u64
			};
			a.send_close_tree_after(busy_ms, &state_file);
			a.wait_until("A to begin", 60_000.0, |a| !a.events("spinning").is_empty()).await;
			a.terminate();
			a.terminated_at()
		};
		let b = Child::spawn("B").await;
		let started = asked.unwrap_or_else(now);
		let mounted = b.mount(&db, Wait::Ms(RELEASE_WAIT_MS)).await;
		let waited = now() - started;
		assert!(
			mounted.ok,
			"{test} round {round}: B could not mount {waited:.0} ms after A was asked to end ({}): {}",
			mounted.kind, mounted.message
		);
		let state = if asked.is_some() {
			console_log!("TERMINATE_WAIT test={test} round={round} ms={waited:.0}");
			waits.push(format!("{waited:.0}"));
			match take_side_file(&state_file).await.first() {
				Some(0) | None => CloseState::Before,
				Some(1) => CloseState::Inside,
				Some(_) => CloseState::After,
			}
		} else {
			CloseState::After
		};
		let opened = b.open().await.unwrap("B opens");
		// What the close had reached decides what is left to replay: a close that returned flushed
		// the memtable, and one that had not begun flushed nothing.
		match state {
			CloseState::Before => assert!(
				opened.number("replayed") > 0,
				"{test} round {round}: nothing was replayed after a worker that had not begun to close"
			),
			CloseState::After => assert_eq!(
				opened.number("replayed"),
				0,
				"{test} round {round}: a log was replayed after a close that had returned"
			),
			CloseState::Inside => {}
		}
		states.push(state);
		let probe = b.probe().await;
		assert!(
			probe.is_a_prefix() && probe.prefix == INDICES,
			"{test} round {round}: B found {probe:?}, expected all {INDICES} durable commits"
		);
		b.verify(0, INDICES).await.unwrap("B reads everything with get");
		console_log!(
			"KILL test={test} round={round} stopped={state:?} files_at_mount={} cut_back={} \
			 replayed={} segments={} tables_at_open={} trimmed={}",
			mounted.number("files"),
			mounted.number("cut_back"),
			opened.number("replayed"),
			opened.number("segments"),
			opened.number("tables"),
			opened.number("trimmed")
		);
		b.commit(&CommitSpec::new(INDICES, AFTER_KILL, true).per(10)).await.unwrap("B commits");
		b.close().await.unwrap("B closes");
		b.exit().await.unwrap("B exits");
		let c = Child::spawn("C").await;
		c.mount_and_open(&db).await;
		c.verify(0, INDICES + AFTER_KILL).await.unwrap("C reads everything");
		c.close().await.unwrap("C closes");
		c.exit().await.unwrap("C exits");
		destroy_when_released(&db.name, REMOVE_WAIT_MS).await;
	}
	let count = |wanted| states.iter().filter(|s| **s == wanted).count();
	let inside = count(CloseState::Inside);
	console_log!(
		"SUMMARY test={test} waits_ms=[{}] stopped_before_the_close={} stopped_inside_the_close={inside} \
		 stopped_after_the_close={}",
		waits.join(","),
		count(CloseState::Before),
		count(CloseState::After)
	);
	assert!(inside >= 1, "no worker was stopped inside the close: {states:?}");
	end_test(test);
}

#[wasm_bindgen_test]
async fn a_worker_killed_in_the_middle_of_close_leaves_every_durable_commit_with_the_value_log() {
	kill_in_close("kill_in_close_with_the_value_log", true).await;
}

#[wasm_bindgen_test]
async fn a_worker_killed_in_the_middle_of_close_leaves_every_durable_commit_without_the_value_log()
{
	kill_in_close("kill_in_close_without_the_value_log", false).await;
}

// ---------------------------------------------------------------------------------------------
// 5. Two databases at once
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn two_workers_work_on_two_databases_at_once_and_do_not_disturb_each_other() {
	const SECONDS: u64 = 4;
	let test = "two_databases_at_once";
	begin_test(test);
	let x = Db::new("two-x", true).with_tag("x");
	let y = Db::new("two-y", true).with_tag("y");
	let a = Child::spawn("A").await;
	let b = Child::spawn("B").await;
	a.mount_and_open(&x).await;
	b.mount_and_open(&y).await;

	let on_a = a.send(&format!("work secs={SECONDS}"));
	let on_b = b.send(&format!("work secs={SECONDS}"));
	let limit = (SECONDS * 1000 + 120_000) as f64;
	let (done_a, done_b) =
		tokio::join!(a.wait(on_a, limit, "A's work on X"), b.wait(on_b, limit, "B's work on Y"));
	let (done_a, done_b) = (done_a.unwrap("A's work"), done_b.unwrap("B's work"));

	// They really ran at the same time: their windows overlap for most of the time.
	let first = done_a.number("first").max(done_b.number("first"));
	let last = done_a.number("last").min(done_b.number("last"));
	let overlap = last - first;
	console_log!(
		"INTERLEAVED overlap_ms={overlap} a_commits={} a_reads={} b_commits={} b_reads={}",
		done_a.number("commits"),
		done_a.number("reads"),
		done_b.number("commits"),
		done_b.number("reads")
	);
	assert!(
		overlap >= (SECONDS * 1000 * 6 / 10) as i64,
		"the two workers ran {overlap} ms at the same time out of {SECONDS} s"
	);
	assert!(done_a.number("commits") > 0 && done_b.number("commits") > 0);

	// Each has its own data, all of it, and none of the other's.
	for (child, db, done) in [(&a, &x, &done_a), (&b, &y, &done_b)] {
		let end = done.number("end") as u64;
		child.verify(0, end).await.unwrap("read back what it wrote");
		let probe = child.probe().await;
		assert!(
			probe.is_a_prefix() && probe.prefix == end,
			"{}: {probe:?}, expected {end}",
			db.name
		);
		child.close().await.unwrap("close");
		child.exit().await.unwrap("exit");
	}
	// Both are still there, and each is the same when read by a worker that was not involved.
	for (db, name, end) in
		[(&x, "RX", done_a.number("end") as u64), (&y, "RY", done_b.number("end") as u64)]
	{
		let reader = Child::spawn(name).await;
		reader.mount_and_open(db).await;
		let probe = reader.probe().await;
		assert!(
			probe.is_a_prefix() && probe.prefix == end,
			"{}: {probe:?}, expected {end}",
			db.name
		);
		reader.close().await.unwrap("close");
		reader.exit().await.unwrap("exit");
	}
	destroy_when_released(&x.name, REMOVE_WAIT_MS).await;
	finish(test, &y).await;
}

// ---------------------------------------------------------------------------------------------
// 6. The move out of IndexedDB, killed
// ---------------------------------------------------------------------------------------------

/// How far into a copy that lasts as long as a whole one the kills of the move come, in tenths of
/// that time, one after the other.
const MIGRATION_TENTHS: [u64; 6] = [0, 1, 3, 5, 7, 9];

/// Rounds of the test of the move: every moment of the list above at least once.
const MIGRATION_ROUNDS: usize = if ROUNDS * 2 > MIGRATION_TENTHS.len() {
	ROUNDS * 2
} else {
	MIGRATION_TENTHS.len()
};

#[wasm_bindgen_test]
async fn a_move_out_of_indexeddb_that_a_kill_cut_short_is_made_again_by_the_next_call() {
	let test = "killed_migration";
	begin_test(test);
	console_log!("{test}: {MIGRATION_ROUNDS} rounds, seed {SEED:#x}");
	let idb_name = unique_name("legacy-killed", "idb");
	let rows = idb::source_rows(3_000);
	let total_bytes: u64 = rows.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();
	let wanted_digest = format!("{:016x}", idb::rows_digest(&rows));
	idb::create_source(&idb_name, &rows).await;
	let before = idb::fingerprint(&idb_name).await;
	assert_eq!(before.rows, rows.len() as u64);
	assert_eq!(
		format!("{:016x}", before.digest),
		wanted_digest,
		"the source is not what was put in it"
	);

	let target = |label: &str| Db::new(label, true).with_mem(256 * 1024).with_tag("moved");
	let migrate = |child: &Child, db: &Db, small: bool| {
		child.send(&format!("migrate idb={idb_name} small={} {}", small as u8, db.args()))
	};

	// One copy to the end, to learn how long it lasts and to see that the reader below finds what
	// a copy leaves. It is cut into small pieces, which makes it last longer.
	let calibration = target("two-moved-calibration");
	let k = Child::spawn("K").await;
	let id = migrate(&k, &calibration, true);
	let done = k.wait(id, 300_000.0, "the whole copy").await.unwrap("the whole copy");
	assert_eq!(done.text("result"), "imported");
	assert_eq!(done.number("entries"), rows.len() as i64);
	assert_eq!(done.number("bytes") as u64, total_bytes);
	let copy_ms = done.number("ms").max(1) as u64;
	k.exit().await;
	check_database(&calibration, rows.len(), &wanted_digest, "the calibration copy").await;
	assert_eq!(idb::fingerprint(&idb_name).await, before, "the copy changed the source");
	destroy_when_released(&calibration.name, REMOVE_WAIT_MS).await;
	console_log!("KILL test={test} a whole copy takes {copy_ms} ms");

	let (mut imported, mut existing, mut finished_before_kill) = (0, 0, 0);
	let (mut markers_left, mut finished_unseen) = (0, 0);
	let mut waits = Vec::new();
	for round in 0..MIGRATION_ROUNDS {
		let tenths = MIGRATION_TENTHS[round % MIGRATION_TENTHS.len()];
		let db = target("two-moved");
		let a = Child::spawn("A").await;
		migrate(&a, &db, true);
		let delay = copy_ms * tenths / 10
			+ if tenths == 0 {
				0
			} else {
				moment(test, round, copy_ms / 20 + 1)
			};
		sleep_ms(delay as f64).await;
		a.terminate();
		// Whether A had finished is what its last events say: one that was still in the air when it
		// was ended is lost.
		let a_finished = !a.events("migrated").is_empty();
		let ended = a.terminated_at().expect("A was ended");
		if a_finished {
			finished_before_kill += 1;
		}

		// What the kill left: a worker that only mounts the files, if there are any, and looks for
		// the marker. It also says how long the browser took to release the files of A.
		let mut marker_left = None;
		if list_directory(&db.name).await.is_some() {
			let inspector = Child::spawn("I").await;
			let mounted = inspector.mount(&db, Wait::Ms(RELEASE_WAIT_MS)).await;
			let waited = now() - ended;
			assert!(
				mounted.ok,
				"{test} round {round}: the files were not released {waited:.0} ms after A was ended: {}",
				mounted.message
			);
			console_log!("TERMINATE_WAIT test={test} round={round} ms={waited:.0}");
			waits.push(format!("{waited:.0}"));
			let marker = inspector.call("marker", "look for the marker").await.unwrap("marker");
			marker_left = Some(marker.text("present") == "1");
			inspector.close().await.unwrap("the inspector closes");
			inspector.exit().await;
		}
		if marker_left == Some(true) {
			markers_left += 1;
		}

		// B calls it again. It cannot ask for a longer wait, so it calls again if the browser has
		// not released the files of A yet.
		let b = Child::spawn("B").await;
		let mut attempts = 0;
		let reply = loop {
			attempts += 1;
			let reply =
				b.call(&format!("migrate idb={idb_name} small=0 {}", db.args()), "B's move").await;
			if reply.is_locked() && now() - ended < RELEASE_WAIT_MS as f64 {
				continue;
			}
			break reply;
		};
		let reply = reply.unwrap("B's move after the kill");
		console_log!(
			"KILL test={test} round={round} killed_after={delay}ms_of_{copy_ms}ms a_had_finished={a_finished} \
			 marker_left={marker_left:?} b_result={} b_attempts={attempts} b_ms={}",
			reply.text("result"),
			reply.number("ms")
		);
		match reply.text("result") {
			"imported" => {
				assert!(!a_finished, "A had finished and B copied again");
				assert_eq!(reply.number("entries"), rows.len() as i64);
				assert_eq!(reply.number("bytes") as u64, total_bytes);
				imported += 1;
			}
			"existing" => {
				// A removed the marker and answered, and the kill came before the answer arrived.
				assert_ne!(
					marker_left,
					Some(true),
					"round {round}: a marker was there and B kept the database"
				);
				if !a_finished {
					finished_unseen += 1;
				}
				existing += 1;
			}
			other => panic!("{test} round {round}: B's move ended with {other:?}"),
		}
		b.exit().await;
		// Either way: the database is the source, no marker is left and the source is as it was.
		check_database(&db, rows.len(), &wanted_digest, &format!("round {round}")).await;
		assert_eq!(idb::fingerprint(&idb_name).await, before, "round {round}: the source changed");
		destroy_when_released(&db.name, REMOVE_WAIT_MS).await;
	}
	console_log!(
		"SUMMARY test={test} copy_ms={copy_ms} waits_ms=[{}] imported_by_b={imported} existing={existing} \
		 finished_before_the_kill={finished_before_kill} finished_but_unseen={finished_unseen} \
		 markers_left_by_the_kill={markers_left}",
		waits.join(",")
	);
	assert!(imported >= 1, "no kill landed inside a copy: B never had to copy again");
	assert!(markers_left >= 1, "no kill left the marker of an unfinished copy");
	watchdog("removing the legacy database", idb::delete_source(&idb_name)).await;
	end_test(test);
}

/// Opens the database in a worker of its own and checks that it holds the rows of the source and
/// no marker.
async fn check_database(db: &Db, rows: usize, digest: &str, what: &str) {
	let reader = Child::spawn("R").await;
	reader.mount(db, Wait::Default).await.unwrap("the reader mounts");
	let marker = reader.call("marker", "look for the marker").await.unwrap("marker");
	assert_eq!(
		marker.text("present"),
		"0",
		"{what}: the marker of the copy is still there: {marker:?}"
	);
	// The engine refuses a database that has the marker, so this open is a second proof.
	reader.open().await.unwrap("the engine opens the database");
	let scan = reader.call("digest", "scan the database").await.unwrap("scan");
	assert_eq!(scan.number("rows"), rows as i64, "{what}: the rows of the database");
	assert_eq!(scan.text("digest"), digest, "{what}: the database is not the source");
	reader.close().await.unwrap("the reader closes");
	reader.exit().await;
}
