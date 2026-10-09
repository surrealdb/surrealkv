//! Adversarial tests for the retire watermark (`commit_pin` reading the completed prefix, and
//! `retire` running only every `RETIRE_EVERY_GROUPS` groups or `RETIRE_EVERY_ENTRIES` entries).
//!
//! What they pin down:
//!
//! * Isolation: no lost update, no write skew with locked reads, and a long-lived transaction that
//!   is lapped by the ring many times still finds every conflict (and is never failed spuriously,
//!   since validation treats a missing entry as a conflict).
//! * The core safety property: the retired watermark never passes the conflict window of a live
//!   mutating transaction, including when `begin` races `retire`.
//! * Liveness: nothing a committer waits for depends on `retire`, so a burst followed by silence
//!   never stalls later committers.
//! * Memory: what is retained while a transaction is open and while the flusher is idle. The
//!   `characterize_*` tests record today's behaviour, which is not bounded.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempdir::TempDir;

use crate::batch::Batch;
use crate::lsm::Tree;
use crate::ring::{PipelineHook, COMMIT_RING_CAPACITY, RETIRE_EVERY_GROUPS};
use crate::{Error, InternalKeyKind, Key, Mode, TreeBuilder};

const CAP: u64 = COMMIT_RING_CAPACITY as u64;

pub(crate) fn create_store() -> (Arc<Tree>, TempDir) {
	let temp_dir = TempDir::new("retire_isolation").unwrap();
	let tree = TreeBuilder::new().with_path(temp_dir.path().to_path_buf()).build().unwrap();
	(Arc::new(tree), temp_dir)
}

/// Fails the test, rather than hanging it, if `fut` does not finish in `secs` seconds.
pub(crate) async fn within<T>(secs: u64, what: &str, fut: impl Future<Output = T>) -> T {
	match tokio::time::timeout(Duration::from_secs(secs), fut).await {
		Ok(v) => v,
		Err(_) => panic!("STALL: {what} did not finish in {secs}s"),
	}
}

/// Commits `n` single-key transactions one after the other, so each is its own group.
pub(crate) async fn commit_unique(store: &Tree, tag: &str, n: u64) {
	for i in 0..n {
		let mut txn = store.begin().unwrap();
		txn.set(format!("{tag}{i}").as_bytes(), b"v").unwrap();
		txn.commit().await.unwrap();
	}
}

fn completed(store: &Tree) -> u64 {
	store.core.commit_pipeline.ring.completed()
}

/// Waits until the completed prefix reaches ring sequence `n`: a committer is woken before the
/// flusher advances the prefix over its entry, so it can lag the last commit that returned.
async fn wait_for_completed(store: &Tree, n: u64) {
	within(30, "the completed prefix to reach the last commit", async {
		while completed(store) < n {
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
	})
	.await;
}

fn taken(store: &Tree) -> u64 {
	store.core.commit_pipeline.ring.taken()
}

/// Waits until the flusher has freed every entry the overflow map holds, which it does without
/// further commits once no live transaction can reach them.
pub(crate) async fn overflow_drained(store: &Tree) {
	let pipeline = &store.core.commit_pipeline;
	within(30, "the flusher to free the retired entries", async {
		while pipeline.watermarks().2 != 0 {
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
	})
	.await;
}

fn enc(v: i64) -> [u8; 8] {
	v.to_be_bytes()
}

fn dec(v: Option<Vec<u8>>) -> i64 {
	v.map(|v| i64::from_be_bytes(v.as_slice().try_into().unwrap())).unwrap_or(0)
}

/// Commits one batch through the pipeline with an explicit snapshot and conflict window, so a
/// test can run the steps of `Transaction::new` one at a time.
async fn raw_commit(
	store: &Tree,
	key: &[u8],
	start_seq: u64,
	window: u64,
	read_set: &[Key],
) -> crate::Result<()> {
	let mut batch = Batch::new(0);
	batch.add_record(InternalKeyKind::Set, key.to_vec(), Some(b"v".to_vec()), 0)?;
	store.core.commit(batch, false, start_seq, window, read_set).await
}

// ---------------------------------------------------------------------------------------------
// (1) Lost updates
// ---------------------------------------------------------------------------------------------

/// `tasks` tasks do read-modify-write increments of three counters, retrying on conflict, while
/// two kinds of transaction are held open across more than a lap of the ring:
///
/// * a contended one that also increments a counter from its stale read (if it were ever allowed to
///   commit, an update would be lost);
/// * a disjoint one that writes a key nobody else touches (it must always commit: a failure means a
///   ring entry its window reaches was lost).
async fn run_counters(locked: bool, tasks: u64, per_task: u64) {
	let (store, _dir) = create_store();
	let successes: Arc<[AtomicU64; 3]> = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
	let stop = Arc::new(AtomicBool::new(false));
	let disjoint_ok = Arc::new(AtomicU64::new(0));
	let contended_attempts = Arc::new(AtomicU64::new(0));

	// Long-lived, contended
	let long_contended = {
		let (store, successes, stop, attempts) = (
			Arc::clone(&store),
			Arc::clone(&successes),
			Arc::clone(&stop),
			Arc::clone(&contended_attempts),
		);
		tokio::spawn(async move {
			while !stop.load(Ordering::Relaxed) {
				let mut txn = store.begin().unwrap();
				let cur = dec(txn.get_for_update(b"c0").unwrap());
				let from = completed(&store);
				let t0 = Instant::now();
				while completed(&store) < from + CAP + CAP / 2
					&& !stop.load(Ordering::Relaxed)
					&& t0.elapsed() < Duration::from_secs(30)
				{
					tokio::time::sleep(Duration::from_millis(1)).await;
				}
				txn.set(b"c0", &enc(cur + 1)).unwrap();
				attempts.fetch_add(1, Ordering::Relaxed);
				match txn.commit().await {
					Ok(()) => {
						successes[0].fetch_add(1, Ordering::SeqCst);
					}
					Err(Error::TransactionWriteConflict) => {}
					Err(e) => panic!("unexpected commit error: {e:?}"),
				}
			}
		})
	};

	// Long-lived, disjoint
	let long_disjoint = {
		let (store, stop, ok) = (Arc::clone(&store), Arc::clone(&stop), Arc::clone(&disjoint_ok));
		tokio::spawn(async move {
			let mut n = 0u64;
			while !stop.load(Ordering::Relaxed) {
				let mut txn = store.begin().unwrap();
				let from = completed(&store);
				let t0 = Instant::now();
				while completed(&store) < from + CAP + CAP / 2
					&& !stop.load(Ordering::Relaxed)
					&& t0.elapsed() < Duration::from_secs(30)
				{
					tokio::time::sleep(Duration::from_millis(1)).await;
				}
				txn.set(format!("quiet{n}").as_bytes(), b"v").unwrap();
				n += 1;
				txn.commit().await.expect(
					"a disjoint long-lived transaction must commit: its window lost an entry",
				);
				ok.fetch_add(1, Ordering::Relaxed);
			}
		})
	};

	let workers: Vec<_> = (0..tasks)
		.map(|t| {
			let (store, successes) = (Arc::clone(&store), Arc::clone(&successes));
			tokio::spawn(async move {
				let mut rng = fastrand::Rng::with_seed(0x5eed + t);
				let mut done = 0;
				while done < per_task {
					let k = rng.usize(0..3);
					let key = format!("c{k}");
					let mut txn = store.begin().unwrap();
					let read = if locked || rng.bool() {
						txn.get_for_update(key.as_bytes())
					} else {
						txn.get(key.as_bytes())
					};
					let cur = dec(read.unwrap());
					if rng.u8(0..8) == 0 {
						tokio::task::yield_now().await;
					}
					txn.set(key.as_bytes(), &enc(cur + 1)).unwrap();
					match txn.commit().await {
						Ok(()) => {
							done += 1;
							successes[k].fetch_add(1, Ordering::SeqCst);
						}
						Err(Error::TransactionWriteConflict) => {}
						Err(e) => panic!("unexpected commit error: {e:?}"),
					}
				}
			})
		})
		.collect();

	within(120, "counter workers", async {
		for w in workers {
			w.await.unwrap();
		}
	})
	.await;
	stop.store(true, Ordering::Relaxed);
	within(60, "long-lived transactions", async {
		long_contended.await.unwrap();
		long_disjoint.await.unwrap();
	})
	.await;

	let txn = store.begin_with_mode(Mode::ReadOnly).unwrap();
	for k in 0..3 {
		let v = dec(txn.get(format!("c{k}").as_bytes()).unwrap());
		let expected = successes[k].load(Ordering::SeqCst);
		assert_eq!(v as u64, expected, "counter c{k}: a committed increment was lost");
	}
	assert!(
		completed(&store) > 3 * CAP,
		"the run must lap the ring several times ({})",
		completed(&store)
	);
	assert!(
		disjoint_ok.load(Ordering::Relaxed) >= 1,
		"no disjoint long-lived transaction completed; the run did not exercise lapped windows"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn counters_with_plain_reads_never_lose_updates_beside_lapped_transactions() {
	run_counters(false, 16, 600).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn counters_with_locked_reads_never_lose_updates_beside_lapped_transactions() {
	run_counters(true, 16, 600).await;
}

// ---------------------------------------------------------------------------------------------
// (2) Bank transfers and write skew
// ---------------------------------------------------------------------------------------------

fn account(i: usize) -> Vec<u8> {
	format!("acct{i}").into_bytes()
}

/// Random transfers between accounts through locked reads of both ends, with a snapshot reader
/// checking the total constantly and a transaction held across several laps.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bank_transfers_keep_the_total_and_every_ledger_balance() {
	const ACCOUNTS: usize = 6;
	const INITIAL: i64 = 1000;
	let (store, _dir) = create_store();
	{
		let mut txn = store.begin().unwrap();
		for i in 0..ACCOUNTS {
			txn.set(account(i), &enc(INITIAL)).unwrap();
		}
		txn.commit().await.unwrap();
	}
	let ledger: Arc<Vec<AtomicI64>> = Arc::new((0..ACCOUNTS).map(|_| AtomicI64::new(0)).collect());
	let stop = Arc::new(AtomicBool::new(false));
	let checks = Arc::new(AtomicU64::new(0));

	// A transaction that locked-read account 0 before anything moved and commits at the end:
	// account 0 changes in between, so it must be refused however many laps went by.
	let mut stale = store.begin().unwrap();
	assert_eq!(dec(stale.get_for_update(account(0)).unwrap()), INITIAL);
	stale.set(b"stale-out", b"v").unwrap();

	let checker = {
		let (store, stop, checks) = (Arc::clone(&store), Arc::clone(&stop), Arc::clone(&checks));
		tokio::spawn(async move {
			while !stop.load(Ordering::Relaxed) {
				let txn = store.begin_with_mode(Mode::ReadOnly).unwrap();
				let total: i64 = (0..ACCOUNTS).map(|i| dec(txn.get(account(i)).unwrap())).sum();
				assert_eq!(total, INITIAL * ACCOUNTS as i64, "a snapshot saw a torn transfer");
				checks.fetch_add(1, Ordering::Relaxed);
				tokio::task::yield_now().await;
			}
		})
	};

	let workers: Vec<_> = (0..12u64)
		.map(|t| {
			let (store, ledger) = (Arc::clone(&store), Arc::clone(&ledger));
			tokio::spawn(async move {
				let mut rng = fastrand::Rng::with_seed(0xbadc0de + t);
				let mut done = 0;
				while done < 400 {
					let from = rng.usize(0..ACCOUNTS);
					let to = (from + 1 + rng.usize(0..ACCOUNTS - 1)) % ACCOUNTS;
					let amount = rng.i64(1..50);
					let mut txn = store.begin().unwrap();
					let a = dec(txn.get_for_update(account(from)).unwrap());
					let b = dec(txn.get_for_update(account(to)).unwrap());
					if rng.u8(0..8) == 0 {
						tokio::task::yield_now().await;
					}
					txn.set(account(from), &enc(a - amount)).unwrap();
					txn.set(account(to), &enc(b + amount)).unwrap();
					match txn.commit().await {
						Ok(()) => {
							done += 1;
							ledger[from].fetch_sub(amount, Ordering::SeqCst);
							ledger[to].fetch_add(amount, Ordering::SeqCst);
						}
						Err(Error::TransactionWriteConflict) => {}
						Err(e) => panic!("unexpected commit error: {e:?}"),
					}
				}
			})
		})
		.collect();
	within(120, "bank workers", async {
		for w in workers {
			w.await.unwrap();
		}
	})
	.await;
	stop.store(true, Ordering::Relaxed);
	checker.await.unwrap();

	assert!(completed(&store) > 2 * CAP, "the run must lap the ring ({})", completed(&store));
	assert!(checks.load(Ordering::Relaxed) > 0);
	assert!(
		matches!(stale.commit().await, Err(Error::TransactionWriteConflict)),
		"a transaction whose locked read was overwritten across laps must be refused"
	);

	let txn = store.begin_with_mode(Mode::ReadOnly).unwrap();
	let mut total = 0;
	for i in 0..ACCOUNTS {
		let balance = dec(txn.get(account(i)).unwrap());
		assert_eq!(
			balance,
			INITIAL + ledger[i].load(Ordering::SeqCst),
			"account {i} does not match the committed transfers"
		);
		total += balance;
	}
	assert_eq!(total, INITIAL * ACCOUNTS as i64);
}

/// The classic write skew: two doctors may each go off call only if the other is on. With
/// locked reads of both rows the two can never both commit. Rounds are interleaved with
/// enough filler commits to lap the ring several times while one transaction stays open.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn write_skew_with_locked_reads_never_commits_both_across_laps() {
	let (store, _dir) = create_store();
	let set_both_on = |store: Arc<Tree>| async move {
		let mut txn = store.begin().unwrap();
		txn.set(b"d0", b"on").unwrap();
		txn.set(b"d1", b"on").unwrap();
		txn.commit().await.unwrap();
	};
	set_both_on(Arc::clone(&store)).await;

	let mut long = store.begin().unwrap();
	assert_eq!(long.get_for_update(b"d0").unwrap().unwrap(), b"on");
	long.set(b"long-out", b"v").unwrap();

	async fn go_off(store: Arc<Tree>, me: usize) -> bool {
		let mut txn = store.begin().unwrap();
		let d0 = txn.get_for_update(b"d0").unwrap().unwrap();
		let d1 = txn.get_for_update(b"d1").unwrap().unwrap();
		if d0 != b"on" || d1 != b"on" {
			return false;
		}
		tokio::task::yield_now().await;
		txn.set(format!("d{me}").as_bytes(), b"off").unwrap();
		match txn.commit().await {
			Ok(()) => true,
			Err(Error::TransactionWriteConflict) => false,
			Err(e) => panic!("unexpected commit error: {e:?}"),
		}
	}

	let (mut both_failed, mut one_went_off) = (0, 0);
	for round in 0..220 {
		set_both_on(Arc::clone(&store)).await;
		let a = tokio::spawn(go_off(Arc::clone(&store), 0));
		let b = tokio::spawn(go_off(Arc::clone(&store), 1));
		let (a, b) =
			within(30, "write skew round", async { (a.await.unwrap(), b.await.unwrap()) }).await;
		assert!(!(a && b), "write skew: both doctors went off call in round {round}");
		match (a, b) {
			(false, false) => both_failed += 1,
			_ => one_went_off += 1,
		}
		let txn = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let on = [b"d0", b"d1"]
			.iter()
			.filter(|k| txn.get(k.as_slice()).unwrap().unwrap() == b"on")
			.count();
		assert!(on >= 1, "nobody left on call after round {round}");
		commit_unique(&store, &format!("fill{round}_"), 20).await;
	}
	assert!(completed(&store) > 4 * CAP, "must lap the ring more than four times");
	assert!(one_went_off > 0, "the rounds never let a doctor go off call ({both_failed} aborted)");
	assert!(
		matches!(long.commit().await, Err(Error::TransactionWriteConflict)),
		"the long-lived transaction's locked read of d0 was overwritten in every round"
	);
}

// ---------------------------------------------------------------------------------------------
// (3) A transaction lapped by the ring still finds its conflict
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Kind {
	/// ReadWrite, writes the contended key.
	WriteWrite,
	/// WriteOnly, writes the contended key.
	WriteOnly,
	/// ReadWrite, locked-reads the contended key and writes elsewhere.
	LockedRead,
}

/// Begins a transaction of `kind`, commits `total` other transactions after it (more than three
/// laps of the ring), one of which writes the contended key at index `conflict_at` if given,
/// and returns the long transaction's verdict.
async fn lapped_verdict(kind: Kind, total: u64, conflict_at: Option<u64>) -> crate::Result<()> {
	let (store, _dir) = create_store();
	// Visible before the transaction begins, so it can never conflict
	{
		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"before").unwrap();
		txn.commit().await.unwrap();
	}
	let mut long = match kind {
		Kind::WriteOnly => store.begin_with_mode(Mode::WriteOnly).unwrap(),
		_ => store.begin().unwrap(),
	};
	match kind {
		Kind::WriteWrite | Kind::WriteOnly => long.set(b"k", b"long").unwrap(),
		Kind::LockedRead => {
			assert_eq!(long.get_for_update(b"k").unwrap().unwrap(), b"before");
			long.set(b"elsewhere", b"long").unwrap();
		}
	}
	for i in 0..total {
		let mut txn = store.begin().unwrap();
		if conflict_at == Some(i) {
			txn.set(b"k", b"other").unwrap();
		} else {
			txn.set(format!("filler{i}").as_bytes(), b"v").unwrap();
		}
		txn.commit().await.unwrap();
	}
	assert!(completed(&store) > 3 * CAP, "the window must span more than three laps");
	long.commit().await
}

const LAPS_3X: u64 = 3 * CAP + 100;

#[tokio::test]
async fn lapped_write_write_conflict_at_the_start_is_found() {
	for kind in [Kind::WriteWrite, Kind::WriteOnly, Kind::LockedRead] {
		let verdict = lapped_verdict(kind, LAPS_3X, Some(0)).await;
		assert!(
			matches!(verdict, Err(Error::TransactionWriteConflict)),
			"{kind:?}: conflict at the first commit of the window was missed: {verdict:?}"
		);
	}
}

#[tokio::test]
async fn lapped_write_write_conflict_in_the_middle_is_found() {
	for kind in [Kind::WriteWrite, Kind::LockedRead] {
		let verdict = lapped_verdict(kind, LAPS_3X, Some(LAPS_3X / 2)).await;
		assert!(
			matches!(verdict, Err(Error::TransactionWriteConflict)),
			"{kind:?}: conflict in the middle of the window was missed: {verdict:?}"
		);
	}
}

#[tokio::test]
async fn lapped_write_write_conflict_at_the_end_is_found() {
	for kind in [Kind::WriteWrite, Kind::LockedRead] {
		let verdict = lapped_verdict(kind, LAPS_3X, Some(LAPS_3X - 1)).await;
		assert!(
			matches!(verdict, Err(Error::TransactionWriteConflict)),
			"{kind:?}: conflict at the last commit of the window was missed: {verdict:?}"
		);
	}
}

/// The converse: with no conflicting commit in the window a lapped transaction must commit, so
/// no entry its window reaches was lost (a lost entry reads as a conflict).
#[tokio::test]
async fn lapped_transaction_without_a_conflict_commits() {
	for kind in [Kind::WriteWrite, Kind::WriteOnly, Kind::LockedRead] {
		let verdict = lapped_verdict(kind, LAPS_3X, None).await;
		// Nobody else writes "k" after the transaction began
		assert!(verdict.is_ok(), "{kind:?}: spurious conflict: {verdict:?}");
	}
}

// ---------------------------------------------------------------------------------------------
// The pin, the window and `retire` racing at the start of a transaction
// ---------------------------------------------------------------------------------------------

/// `Transaction::new` reads the pin, registers it, then reads the window. A `retire` that runs
/// after the pin is read but before it is registered does not see the transaction, so it may
/// pass the pin. It must not pass the window read afterwards. This runs the steps one at a time
/// with a `retire` (forced by committing past `RETIRE_EVERY_GROUPS` groups) in the gap, then
/// laps the ring and checks the transaction still finds its conflict and is not failed
/// spuriously.
#[tokio::test]
async fn retire_between_pin_and_register_cannot_pass_the_window() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	commit_unique(&store, "history", 5).await;

	let pin = pipeline.commit_pin();
	// The gap: not registered, so a retire in here does not see this transaction
	commit_unique(&store, "gap", 3 * u64::from(RETIRE_EVERY_GROUPS)).await;
	let (_, taken_in_gap, _) = pipeline.watermarks();
	assert!(
		taken_in_gap > pin,
		"setup: the retire in the gap must have passed the stale pin ({taken_in_gap} vs {pin})"
	);
	let guard = store.core.active_txn_tracker.register(pin);
	let window = pipeline.commit_window();
	let start_seq = store.core.seq_num();
	assert!(
		taken_in_gap <= window,
		"retired watermark {taken_in_gap} passed the window {window} read after registering"
	);

	let mut other = store.begin().unwrap();
	other.set(b"contended", b"other").unwrap();
	other.commit().await.unwrap();
	commit_unique(&store, "lap", 3 * CAP).await;

	let (completed, taken, overflow) = pipeline.watermarks();
	assert!(taken <= window, "retired watermark {taken} passed the live window {window}");
	assert!(completed - window > 3 * CAP);
	assert!(overflow as u64 >= completed - CAP - window - 64, "overflow {overflow}");

	let verdict = raw_commit(&store, b"contended", start_seq, window, &[]).await;
	assert!(matches!(verdict, Err(Error::TransactionWriteConflict)), "{verdict:?}");
	raw_commit(&store, b"unrelated", start_seq, window, &[]).await.expect("spurious conflict");
	let verdict =
		raw_commit(&store, b"elsewhere", start_seq, window, &[b"contended".to_vec()]).await;
	assert!(matches!(verdict, Err(Error::TransactionWriteConflict)), "locked read: {verdict:?}");
	drop(guard);
}

/// A retire between registering the pin and reading the window sees the pin, so it stops there.
#[tokio::test]
async fn retire_between_register_and_window_stops_at_the_pin() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	commit_unique(&store, "history", 5).await;

	let pin = pipeline.commit_pin();
	let guard = store.core.active_txn_tracker.register(pin);
	commit_unique(&store, "gap", 3 * u64::from(RETIRE_EVERY_GROUPS)).await;
	let (completed_now, taken_now, _) = pipeline.watermarks();
	assert!(completed_now > pin + 2 * u64::from(RETIRE_EVERY_GROUPS));
	assert!(taken_now <= pin, "retire passed a registered pin: {taken_now} > {pin}");
	let window = pipeline.commit_window();
	assert!(pin <= window);
	let start_seq = store.core.seq_num();

	let mut other = store.begin().unwrap();
	other.set(b"contended", b"other").unwrap();
	other.commit().await.unwrap();
	commit_unique(&store, "lap", 3 * CAP).await;
	assert!(pipeline.ring.taken() <= pin);
	let verdict = raw_commit(&store, b"contended", start_seq, window, &[]).await;
	assert!(matches!(verdict, Err(Error::TransactionWriteConflict)), "{verdict:?}");
	drop(guard);
}

/// `retire` reads the completed prefix and only then scans the pins. This parks the flusher
/// between the two (the retire gap hook), begins a transaction in the gap, and advances the
/// completed prefix past the transaction's window without the flusher (locked-read-only commits
/// publish an aborted entry and advance the prefix themselves). The retire that resumes must not
/// pass the window: it read the prefix before the window was read, so the value it bounds by is
/// at or below the window, and the scan that follows sees the registered pin.
///
/// If the two reads were swapped (scan first, read the prefix afterwards) the prefix read after
/// the gap is above the window and this fails. A committer's own pin may still be registered
/// when the scan runs, which hides the bug in a round now and then, so it is repeated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_that_begins_inside_retire_is_not_passed() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;

	// 0 armed, 1 the flusher is parked in the gap, 2 released, 3 the flusher left the gap
	let gate = Arc::new(AtomicUsize::new(3));
	let hook: Arc<dyn Fn() + Send + Sync> = {
		let gate = Arc::clone(&gate);
		Arc::new(move || {
			if gate.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
				while gate.load(Ordering::SeqCst) != 2 {
					std::thread::sleep(Duration::from_millis(1));
				}
				gate.store(3, Ordering::SeqCst);
			}
		})
	};
	pipeline.set_retire_gap_hook(Some(hook));

	for round in 0..10 {
		gate.store(0, Ordering::SeqCst);
		// Sequential commits, one group each, until a retire runs and parks in the gap
		let mut parked = false;
		for i in 0..(2 * RETIRE_EVERY_GROUPS) {
			if gate.load(Ordering::SeqCst) == 1 {
				parked = true;
				break;
			}
			// Not a `Transaction`: its pin would stay registered for a moment after the commit
			// returns, and a scan in that moment would hide the bug this test is after.
			let key = format!("drive{round}_{i}");
			raw_commit(&store, key.as_bytes(), store.core.seq_num(), completed(&store), &[])
				.await
				.unwrap();
			// The retire follows the group that woke the committer
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
		assert!(parked || gate.load(Ordering::SeqCst) == 1, "round {round}: no retire ran");

		// Begin in the gap, then move the completed prefix past the window
		let x = store.begin().unwrap();
		let window = x.commit_window_for_test();
		for _ in 0..8 {
			let mut y = store.begin().unwrap();
			assert!(y.get_for_update(b"nothing").unwrap().is_none());
			within(10, "a locked-read-only commit", y.commit()).await.unwrap();
		}
		assert!(completed(&store) > window, "setup: the prefix must move past the window");

		gate.store(2, Ordering::SeqCst);
		within(10, "the flusher to leave the gap", async {
			while gate.load(Ordering::SeqCst) != 3 {
				tokio::time::sleep(Duration::from_millis(1)).await;
			}
		})
		.await;
		// The retire finishes right after the hook returns
		tokio::time::sleep(Duration::from_millis(20)).await;
		assert!(
			taken(&store) <= window,
			"round {round}: retired watermark {} passed the live window {window}",
			taken(&store)
		);
		drop(x);
	}
	pipeline.set_retire_gap_hook(None);
}

/// A transaction that begins while earlier commits are decided but not yet complete (so the
/// completed prefix, and the pin and window with it, sit below them) must still conflict with
/// them when it writes the same key, and must not conflict with a commit that completed before
/// it began.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn begin_while_a_commit_is_decided_but_not_complete_still_conflicts() {
	let (store, _dir) = create_store();
	commit_unique(&store, "history", 3).await;

	// 0 armed, 1 the flusher is parked after logging, 2 released
	let gate = Arc::new(AtomicUsize::new(0));
	let hook: Arc<dyn Fn(PipelineHook) + Send + Sync> = {
		let gate = Arc::clone(&gate);
		Arc::new(move |point| {
			if matches!(point, PipelineHook::AfterWalSync { .. })
				&& gate.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok()
			{
				while gate.load(Ordering::SeqCst) != 2 {
					std::thread::sleep(Duration::from_millis(1));
				}
			}
		})
	};
	store.core.commit_pipeline.set_hook(Some(hook));

	let writer = {
		let store = Arc::clone(&store);
		tokio::spawn(async move {
			let mut txn = store.begin().unwrap();
			txn.set(b"k", b"decided").unwrap();
			txn.commit().await.unwrap();
		})
	};
	within(10, "flusher to reach the gate", async {
		while gate.load(Ordering::SeqCst) != 1 {
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
	})
	.await;

	// The writer's commit is in the WAL but neither applied nor complete
	let pipeline = &store.core.commit_pipeline;
	let (completed_before, _, _) = pipeline.watermarks();
	let mut racing = store.begin().unwrap();
	let mut racing_locked = store.begin().unwrap();
	assert!(racing.get(b"k").unwrap().is_none(), "the decided commit is not visible yet");
	assert!(racing_locked.get_for_update(b"k").unwrap().is_none());
	racing.set(b"k", b"racing").unwrap();
	racing_locked.set(b"other", b"v").unwrap();
	assert!(racing.commit_window_for_test() <= completed_before + 1);

	gate.store(2, Ordering::SeqCst);
	within(10, "the decided commit", writer).await.unwrap();
	store.core.commit_pipeline.set_hook(None);

	assert!(matches!(racing.commit().await, Err(Error::TransactionWriteConflict)));
	assert!(matches!(racing_locked.commit().await, Err(Error::TransactionWriteConflict)));

	// Begun after the writer completed: its snapshot has the write, so no conflict
	let mut after = store.begin().unwrap();
	assert_eq!(after.get_for_update(b"k").unwrap().unwrap(), b"decided");
	after.set(b"k", b"after").unwrap();
	after.commit().await.unwrap();
}

// ---------------------------------------------------------------------------------------------
// The safety property, monitored under load
// ---------------------------------------------------------------------------------------------

/// Many mutating transactions begin, hold, and commit while the flusher retires. At every
/// sample, for every live transaction, the retired watermark must be at or below its window.
/// Every transaction writes only keys of its own, so a conflict can only mean a ring entry
/// its window reaches was lost, which validation reports as a conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn retired_watermark_never_passes_a_live_window_and_unique_writers_never_conflict() {
	let (store, _dir) = create_store();
	let holders = Arc::new(AtomicUsize::new(0));
	let committed = Arc::new(AtomicU64::new(0));
	let finished = Arc::new(AtomicU64::new(0));
	let tasks = 16u64;
	let iters = 1500u64;

	let workers: Vec<_> = (0..tasks)
		.map(|t| {
			let (store, holders, committed, finished) = (
				Arc::clone(&store),
				Arc::clone(&holders),
				Arc::clone(&committed),
				Arc::clone(&finished),
			);
			tokio::spawn(async move {
				let mut rng = fastrand::Rng::with_seed(0xfeed + t);
				let check = |w: u64, when: &str| {
					let t = taken(&store);
					assert!(t <= w, "retired watermark {t} passed a live window {w} ({when})");
				};
				for i in 0..iters {
					let mode = if rng.bool() {
						Mode::ReadWrite
					} else {
						Mode::WriteOnly
					};
					let mut txn = store.begin_with_mode(mode).unwrap();
					let w = txn.commit_window_for_test();
					check(w, "at begin");
					match rng.u32(0..100) {
						0..=1 if holders.fetch_add(1, Ordering::SeqCst) < 4 => {
							// Hold the window open across more than a lap of the ring
							let from = completed(&store);
							let t0 = Instant::now();
							// Other workers drive the ring; once most are done nothing would
							while completed(&store) < from + CAP + 64
								&& finished.load(Ordering::SeqCst) < tasks - 4
								&& t0.elapsed() < Duration::from_secs(20)
							{
								check(w, "while held across a lap");
								tokio::time::sleep(Duration::from_micros(500)).await;
							}
							holders.fetch_sub(1, Ordering::SeqCst);
						}
						0..=1 => {
							holders.fetch_sub(1, Ordering::SeqCst);
						}
						2..=25 => {
							tokio::task::yield_now().await;
							check(w, "after a yield");
						}
						_ => {}
					}
					if mode == Mode::ReadWrite && rng.u8(0..4) == 0 {
						txn.get_for_update(format!("lock{t}_{i}").as_bytes()).unwrap();
					}
					txn.set(format!("u{t}_{i}").as_bytes(), b"v").unwrap();
					check(w, "before commit");
					match txn.commit().await {
						Ok(()) => {
							committed.fetch_add(1, Ordering::Relaxed);
						}
						Err(e) => {
							panic!("task {t} iteration {i}: a transaction with its own keys failed: {e:?}")
						}
					}
				}
				finished.fetch_add(1, Ordering::SeqCst);
			})
		})
		.collect();
	within(180, "monitored workers", async {
		for w in workers {
			w.await.unwrap();
		}
	})
	.await;
	assert_eq!(committed.load(Ordering::Relaxed), tasks * iters);
	wait_for_completed(&store, tasks * iters).await;
}

// ---------------------------------------------------------------------------------------------
// (4) Liveness: burst, then silence
// ---------------------------------------------------------------------------------------------

/// A burst that fills the whole ring in fewer than `RETIRE_EVERY_GROUPS` groups never retires,
/// and the flusher then goes idle with a full ring of unretired entries. Nothing a committer
/// waits for depends on `retire`: a publisher waits only for the previous occupant of its slot
/// to be complete, and permits come back when the completed prefix passes an entry. So new
/// committers, with a transaction still open, must all proceed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn burst_then_silence_never_stalls_new_committers() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;

	// The flusher dawdles after each group so that committers pile up behind it and every
	// group is large: the burst takes only a handful of groups. It waits until every commit
	// that can be admitted at once, or all that are left of the burst, is accepted and waiting,
	// and gives up after 500 ms, so that the group count does not depend on how busy the
	// machine is.
	let burst = CAP + CAP / 2;
	let groups = Arc::new(AtomicUsize::new(0));
	let hook: Arc<dyn Fn(PipelineHook) + Send + Sync> = {
		let groups = Arc::clone(&groups);
		let store = Arc::clone(&store);
		Arc::new(move |point| {
			if matches!(point, PipelineHook::AfterWalSync { .. }) {
				groups.fetch_add(1, Ordering::SeqCst);
				let pipeline = &store.core.commit_pipeline;
				let admitted = (CAP / 2).min(burst.saturating_sub(pipeline.ring.completed()));
				let deadline = Instant::now() + Duration::from_millis(500);
				while pipeline.accepted_waiting() < admitted as usize && Instant::now() < deadline {
					std::thread::sleep(Duration::from_millis(1));
				}
			}
		})
	};
	pipeline.set_hook(Some(hook));

	let held = store.begin().unwrap();
	let tasks: Vec<_> = (0..burst)
		.map(|i| {
			let store = Arc::clone(&store);
			tokio::spawn(async move {
				let mut txn = store.begin().unwrap();
				txn.set(format!("burst{i}").as_bytes(), b"v").unwrap();
				txn.commit().await.unwrap();
			})
		})
		.collect();
	within(60, "the burst", async {
		for t in tasks {
			t.await.unwrap();
		}
	})
	.await;
	pipeline.set_hook(None);

	let burst_groups = groups.load(Ordering::SeqCst);
	assert!(
		burst_groups < RETIRE_EVERY_GROUPS as usize,
		"setup: the burst took {burst_groups} groups, so retire may have run"
	);
	let (c0, t0, o0) = pipeline.watermarks();
	assert_eq!(pipeline.ring.occupied(), CAP as usize, "the ring is full of unretired entries");
	assert_eq!(t0, 0, "nothing has retired yet");
	eprintln!("after burst: groups={burst_groups} completed={c0} taken={t0} overflow={o0}");

	// Silence: no backstop retires while the flusher is idle
	tokio::time::sleep(Duration::from_millis(400)).await;
	assert_eq!(pipeline.watermarks(), (c0, t0, o0), "something moved while idle");
	assert_eq!(pipeline.ring.occupied(), CAP as usize);

	// New committers, one transaction still open, must not stall
	within(30, "sequential commits after silence", commit_unique(&store, "after", 64)).await;
	let concurrent: Vec<_> = (0..2 * CAP)
		.map(|i| {
			let store = Arc::clone(&store);
			tokio::spawn(async move {
				let mut txn = store.begin().unwrap();
				txn.set(format!("again{i}").as_bytes(), b"v").unwrap();
				txn.commit().await.unwrap();
			})
		})
		.collect();
	within(60, "concurrent commits after silence", async {
		for t in concurrent {
			t.await.unwrap();
		}
	})
	.await;

	// Once the open transaction ends, the next retire (within RETIRE_EVERY_GROUPS groups)
	// catches up and the overflow drains
	drop(held);
	commit_unique(&store, "drain", 2 * u64::from(RETIRE_EVERY_GROUPS)).await;
	overflow_drained(&store).await;
	let (c, t, o) = pipeline.watermarks();
	assert!(c - t <= u64::from(RETIRE_EVERY_GROUPS), "taken {t} lags completed {c}");
	assert_eq!(o, 0, "overflow drains once nothing can reach it");
}

/// A burst that aborts most of its commits (every committer writes the same key from the same
/// window) leaves aborted entries holding admission permits until the flusher drains them. They
/// must all be drained, and later commits must flow, even though no retire ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_burst_of_conflict_aborts_leaves_no_permit_stuck() {
	let (store, _dir) = create_store();
	let (ok, conflict) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
	let n = 4 * CAP;
	let txns: Vec<_> = (0..n).map(|_| store.begin().unwrap()).collect();
	let tasks: Vec<_> = txns
		.into_iter()
		.map(|mut txn| {
			let (ok, conflict) = (Arc::clone(&ok), Arc::clone(&conflict));
			tokio::spawn(async move {
				txn.set(b"hot", b"v").unwrap();
				match txn.commit().await {
					Ok(()) => ok.fetch_add(1, Ordering::Relaxed),
					Err(Error::TransactionWriteConflict) => {
						conflict.fetch_add(1, Ordering::Relaxed)
					}
					Err(e) => panic!("unexpected commit error: {e:?}"),
				};
			})
		})
		.collect();
	within(60, "the conflicting burst", async {
		for t in tasks {
			t.await.unwrap();
		}
	})
	.await;
	assert_eq!(ok.load(Ordering::Relaxed) + conflict.load(Ordering::Relaxed), n);
	assert!(ok.load(Ordering::Relaxed) >= 1);

	tokio::time::sleep(Duration::from_millis(200)).await;
	within(30, "commits after the aborts", commit_unique(&store, "after", 2 * CAP)).await;
}

// ---------------------------------------------------------------------------------------------
// (5) Memory
// ---------------------------------------------------------------------------------------------

/// CHARACTERIZATION, not a bound: while one mutating transaction is open, every commit that
/// laps the ring above its window is kept in the overflow map, so retained entries grow linearly
/// with the commits made during its lifetime. A transaction's conflict window cannot be
/// shortened (it must see every commit after its start), so nothing here can free them.
/// `commit_pin` reading the completed prefix does not change this; it only stops *other*,
/// short transactions from pinning the watermark.
#[tokio::test]
async fn characterize_overflow_grows_linearly_while_one_transaction_is_open() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let held = store.begin().unwrap();
	let window = held.commit_window_for_test();

	let mut samples = Vec::new();
	for round in 1..=5u64 {
		commit_unique(&store, &format!("r{round}_"), CAP).await;
		let (completed, taken, overflow) = pipeline.watermarks();
		assert!(taken <= window, "retired watermark {taken} passed the open window {window}");
		samples.push((round * CAP, completed, taken, overflow));
	}
	for (commits, completed, taken, overflow) in &samples {
		eprintln!("commits={commits} completed={completed} taken={taken} overflow={overflow}");
	}
	let (commits, _, _, overflow) = *samples.last().unwrap();
	// Everything above the window that a lap overwrote is retained
	assert!(
		overflow as u64 >= commits - CAP - 64,
		"{overflow} lapped entries kept for {commits} commits: expected linear growth"
	);
	let early = samples[1].3;
	assert!(
		overflow > early + (CAP as usize),
		"the overflow map must keep growing with the open transaction ({early} then {overflow})"
	);
	drop(held);
}

/// Once the open transaction ends, the first `retire` that runs, which is within
/// `RETIRE_EVERY_GROUPS` groups of the commits that follow, passes the entries it kept, and the
/// flusher then frees them without waiting for more commits. Nothing retires them while the
/// store is idle, which is a limitation and not something to rely on.
#[tokio::test]
async fn overflow_is_freed_within_one_interval_after_the_transaction_ends() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let held = store.begin().unwrap();
	commit_unique(&store, "fill", 4 * CAP).await;
	let (_, _, overflow_open) = pipeline.watermarks();
	assert!(overflow_open as u64 > 2 * CAP);

	drop(held);
	commit_unique(&store, "resume_", u64::from(RETIRE_EVERY_GROUPS)).await;
	overflow_drained(&store).await;
	let (completed, taken, after_interval) = pipeline.watermarks();
	assert_eq!(after_interval, 0, "retire runs within one interval and drains the overflow");
	assert!(completed - taken <= u64::from(RETIRE_EVERY_GROUPS) + 1);
}

/// With short transactions that always overlap (some transaction is live at every moment), the
/// retained entries stay bounded, which is what the change buys.
#[tokio::test]
async fn overlapping_short_transactions_keep_retained_entries_bounded() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let mut live = store.begin().unwrap();
	let mut max_overflow = 0;
	for i in 0..(5 * CAP) {
		let next = store.begin().unwrap();
		live.set(format!("overlap{i}").as_bytes(), b"v").unwrap();
		live.commit().await.unwrap();
		live = next;
		max_overflow = max_overflow.max(pipeline.watermarks().2);
	}
	assert!(max_overflow <= 64, "overflow reached {max_overflow} with only short transactions");
	assert!(pipeline.ring.occupied() <= CAP as usize);
}

/// A read-only transaction opens no conflict window and registers no pin, so it never holds
/// the watermark back however long it lives.
#[tokio::test]
async fn a_long_lived_read_only_transaction_does_not_pin_the_watermark() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let reader = store.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_eq!(store.core.active_txn_tracker.len(), 0);
	commit_unique(&store, "fill", 3 * CAP).await;
	let (completed, taken, overflow) = pipeline.watermarks();
	assert!(completed - taken <= u64::from(RETIRE_EVERY_GROUPS), "{taken} lags {completed}");
	assert_eq!(overflow, 0);
	drop(reader);
}

/// Rolling a transaction back releases its pin at once, so a retire can pass its window.
#[tokio::test]
async fn rollback_releases_the_pin_and_the_retained_entries_drain() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let mut long = store.begin().unwrap();
	long.set(b"x", b"v").unwrap();
	commit_unique(&store, "fill", 3 * CAP).await;
	assert!(pipeline.watermarks().2 as u64 > CAP, "setup: entries are being kept for the window");
	long.rollback();
	assert_eq!(store.core.active_txn_tracker.len(), 0);
	commit_unique(&store, "drain", 2 * u64::from(RETIRE_EVERY_GROUPS)).await;
	overflow_drained(&store).await;
	let (completed, taken, overflow) = pipeline.watermarks();
	assert!(completed - taken <= u64::from(RETIRE_EVERY_GROUPS));
	assert_eq!(overflow, 0);
}

/// Locked reads and writes released by `rollback_to_savepoint` leave the conflict check, those
/// registered before the savepoint stay in it, and neither depends on how many laps passed.
#[tokio::test]
async fn savepoints_decide_which_locked_reads_conflict_across_laps() {
	for (written_by_others, expect_conflict) in [
		(&[b"kept".as_slice(), b"released".as_slice()][..], true),
		(&[b"released".as_slice()][..], false),
	] {
		let (store, _dir) = create_store();
		let mut long = store.begin().unwrap();
		assert!(long.get_for_update(b"kept").unwrap().is_none());
		long.set_savepoint().unwrap();
		assert!(long.get_for_update(b"released").unwrap().is_none());
		long.set(b"rolled-back-write", b"v").unwrap();
		long.rollback_to_savepoint().unwrap();
		long.set(b"out", b"v").unwrap();

		let mut other = store.begin().unwrap();
		for key in written_by_others {
			other.set(*key, b"other").unwrap();
		}
		other.set(b"rolled-back-write", b"other").unwrap();
		other.commit().await.unwrap();
		commit_unique(&store, "lap", 3 * CAP).await;

		let verdict = long.commit().await;
		if expect_conflict {
			assert!(matches!(verdict, Err(Error::TransactionWriteConflict)), "{verdict:?}");
		} else {
			assert!(
				verdict.is_ok(),
				"locks released by the savepoint rollback must not conflict: {verdict:?}"
			);
		}
	}
}

/// A restore clears the overflow map and resets the sequence counters but leaves the ring and
/// its watermarks alone. A transaction open across it finishes (validation reads the cleared
/// entries as conflicts), and transactions begun afterwards get the same guarantees as before:
/// conflicts across laps are found, disjoint transactions commit, and the retained entries drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_with_a_transaction_open_leaves_validation_and_retire_working() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	commit_unique(&store, "base", 10).await;
	store.flush().unwrap();
	let checkpoints = TempDir::new("retire_checkpoint").unwrap();
	let checkpoint = checkpoints.path().join("checkpoint");
	store.create_checkpoint(&checkpoint).unwrap();

	let mut before = store.begin().unwrap();
	assert!(before.get_for_update(b"untouched").unwrap().is_none());
	before.set(b"before-out", b"v").unwrap();
	commit_unique(&store, "post", 2 * CAP).await;
	assert!(pipeline.watermarks().2 > 0, "setup: entries are kept for the open transaction");
	store.restore_from_checkpoint(&checkpoint).unwrap();

	// Whatever its verdict, it must finish and release its pin
	let verdict = within(30, "the pre-restore commit", before.commit()).await;
	eprintln!("pre-restore transaction verdict: {verdict:?}");
	assert_eq!(store.core.active_txn_tracker.len(), 0);

	let mut conflicting = store.begin().unwrap();
	conflicting.set(b"contended", b"mine").unwrap();
	let mut disjoint = store.begin().unwrap();
	disjoint.set(b"disjoint", b"mine").unwrap();
	let mut other = store.begin().unwrap();
	other.set(b"contended", b"other").unwrap();
	other.commit().await.unwrap();
	commit_unique(&store, "lap", 3 * CAP).await;
	assert!(matches!(conflicting.commit().await, Err(Error::TransactionWriteConflict)));
	disjoint.commit().await.expect("a disjoint transaction begun after the restore must commit");

	commit_unique(&store, "drain", 2 * u64::from(RETIRE_EVERY_GROUPS)).await;
	overflow_drained(&store).await;
	let (completed, taken, overflow) = pipeline.watermarks();
	assert!(completed - taken <= u64::from(RETIRE_EVERY_GROUPS), "{taken} lags {completed}");
	assert_eq!(overflow, 0);
}

// ---------------------------------------------------------------------------------------------
// Measurements (ignored: run with `--ignored --nocapture --test-threads=1`, one at a time)
// ---------------------------------------------------------------------------------------------

fn rss_bytes() -> u64 {
	let out = std::process::Command::new("ps")
		.args(["-o", "rss=", "-p", &std::process::id().to_string()])
		.output()
		.unwrap();
	String::from_utf8(out.stdout).unwrap().trim().parse::<u64>().unwrap() * 1024
}

/// Resident memory added by `n` commits, with or without a mutating transaction held open.
async fn rss_after_commits(held: bool, n: u64) -> (u64, usize) {
	let (store, _dir) = create_store();
	// Fill and lap the ring once so its own slots are already paid for
	commit_unique(&store, "warm", 2 * CAP).await;
	let open = held.then(|| store.begin().unwrap());
	let before = rss_bytes();
	commit_unique(&store, "m", n).await;
	let after = rss_bytes();
	let (_, _, overflow) = store.core.commit_pipeline.watermarks();
	drop(open);
	(after.saturating_sub(before), overflow)
}

#[ignore = "measurement"]
#[tokio::test]
async fn measure_rss_of_commits_without_an_open_transaction() {
	let (delta, overflow) = rss_after_commits(false, 30 * CAP).await;
	eprintln!("MEASURE no open txn: rss delta={delta} bytes, overflow={overflow}");
}

#[ignore = "measurement"]
#[tokio::test]
async fn measure_rss_of_commits_with_an_open_transaction() {
	let (delta, overflow) = rss_after_commits(true, 30 * CAP).await;
	eprintln!("MEASURE open txn: rss delta={delta} bytes, overflow={overflow}");
}

/// What the flusher does to the commits that follow a long-lived transaction's end: the entries
/// it kept are freed a chunk at a time between groups, so no commit waits for more than one
/// chunk.
#[ignore = "measurement"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn measure_commit_latency_when_a_retire_frees_a_large_overflow() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	commit_unique(&store, "warm", 2 * CAP).await;
	let held = store.begin().unwrap();
	commit_unique(&store, "fill", 60 * CAP).await;
	let (_, _, overflow) = pipeline.watermarks();
	let mut size = 0;
	pipeline.ring.read(pipeline.ring.completed(), |e| size = std::mem::size_of_val(&**e));
	drop(held);
	let mut worst = Duration::ZERO;
	let mut all = Vec::new();
	for i in 0..(2 * RETIRE_EVERY_GROUPS) {
		let t0 = Instant::now();
		let mut txn = store.begin().unwrap();
		txn.set(format!("after{i}").as_bytes(), b"v").unwrap();
		txn.commit().await.unwrap();
		let took = t0.elapsed();
		all.push(took);
		worst = worst.max(took);
	}
	all.sort();
	eprintln!(
		"MEASURE overflow={overflow} entries (CommitEntry inline size {size} bytes): worst commit \
		 after the transaction ended {worst:?}, median {:?}",
		all[all.len() / 2]
	);
}
