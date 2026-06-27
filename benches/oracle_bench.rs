// Oracle critical-section microbenchmark.
//
// Measures the SERIAL THROUGHPUT CEILING the commit oracle imposes. In
// production `oracle.check`/`publish` always run under the pipeline's
// `write_mutex`, so the oracle's own internal mutex never contends — the only
// thing that matters is how many (check -> seq-alloc -> publish) sequences the
// oracle can sustain when fully serialized. We reproduce that exactly: a
// `Mutex<()>` stands in for `write_mutex`, an `AtomicU64` for the seq counter.
//
// The decisive question (see the plan): is the oracle even the bottleneck? The
// observed end-to-end Create rate is 5M ops / 15.7s ~= 318K commits/sec. If the
// oracle's ceiling here is >> 318K/sec, it is NOT the wall and the per-commit
// WAL write() syscall (also under the lock) is the real cost.
//
// Variants:
//   V0 - current oracle: check() locks, publish() locks (2 acquisitions/commit)
//   V1 - check_and_publish(): one lock acquisition (Fix A-lite)
//
// Distributions:
//   unique - every commit a fresh key (Create-like; conflict check never fires)
//   hot    - small shared key pool (Update-like; exercises conflicts + the
//            abort path)
//
// Run: cargo bench --bench oracle_bench

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Instant;

use surrealkv::CommitOracle;

/// Total commits per configuration (split across the worker threads), matching
/// the crud-bench scale so timing is comparable across the N sweep.
const TOTAL_OPS: u64 = 3_000_000;
/// Steady-state oracle retention window: `oldest_active = seq - RETAIN`, so the
/// GC body (O(N) retain every GC_INTERVAL=1024 commits) prunes to ~RETAIN
/// entries — a realistic map size, exercising the GC cost.
const RETAIN: u64 = 100_000;
/// Hot-key pool size for the `hot` distribution.
const HOT_KEYS: u64 = 1024;

#[derive(Clone, Copy)]
enum Variant {
	/// check() then publish() — two lock acquisitions (current production).
	TwoLock,
	/// check_and_publish() — single lock acquisition (Fix A-lite).
	OneLock,
}

#[derive(Clone, Copy)]
enum Dist {
	Unique,
	Hot,
}

/// Write a 26-byte zero-padded decimal key (crud-bench uses `-k string26`),
/// alloc-free, into `buf`.
#[inline]
fn write_key(buf: &mut [u8; 26], mut n: u64) {
	for i in (0..26).rev() {
		buf[i] = b'0' + (n % 10) as u8;
		n /= 10;
	}
}

/// Run one configuration; returns (commits/sec, conflicts observed).
fn run(variant: Variant, dist: Dist, n_threads: u64) -> (f64, u64) {
	let oracle = Arc::new(CommitOracle::new());
	let write_mutex = Arc::new(Mutex::new(()));
	let seq = Arc::new(AtomicU64::new(1));
	let barrier = Arc::new(Barrier::new(n_threads as usize + 1));
	let ops_per_thread = TOTAL_OPS / n_threads;

	let mut handles = Vec::with_capacity(n_threads as usize);
	for tid in 0..n_threads {
		let oracle = Arc::clone(&oracle);
		let write_mutex = Arc::clone(&write_mutex);
		let seq = Arc::clone(&seq);
		let barrier = Arc::clone(&barrier);
		handles.push(thread::spawn(move || {
			let mut buf = [0u8; 26];
			let mut conflicts: u64 = 0;
			barrier.wait();
			for i in 0..ops_per_thread {
				// Key per distribution.
				let key_id = match dist {
					// Disjoint per-thread ranges => globally unique.
					Dist::Unique => tid * ops_per_thread + i,
					Dist::Hot => (tid.wrapping_mul(2_654_435_761).wrapping_add(i)) % HOT_KEYS,
				};
				write_key(&mut buf, key_id);
				let keys = [buf.as_slice()];

				// Fresh snapshot each iteration so `start_seq` stays well above
				// `kept_since` (we want WriteConflict on hot keys, not Retry).
				let start_seq = seq.load(Ordering::Relaxed);

				let _g = write_mutex.lock().unwrap();
				match variant {
					Variant::TwoLock => {
						if oracle.check(keys, start_seq).is_err() {
							conflicts += 1;
							continue; // abort: no seq consumed, no publish (production behavior)
						}
						let s = seq.fetch_add(1, Ordering::SeqCst);
						let oldest_active = s.saturating_sub(RETAIN);
						oracle.publish(keys, s, 1, oldest_active);
					}
					Variant::OneLock => {
						let s = seq.fetch_add(1, Ordering::SeqCst);
						let oldest_active = s.saturating_sub(RETAIN);
						if oracle.check_and_publish(keys, start_seq, s, 1, oldest_active).is_err() {
							conflicts += 1;
						}
					}
				}
			}
			conflicts
		}));
	}

	barrier.wait();
	let t0 = Instant::now();
	let mut conflicts = 0;
	for h in handles {
		conflicts += h.join().unwrap();
	}
	let elapsed = t0.elapsed().as_secs_f64();
	let total = ops_per_thread * n_threads;
	(total as f64 / elapsed, conflicts)
}

fn variant_name(v: Variant) -> &'static str {
	match v {
		Variant::TwoLock => "V0 two-lock ",
		Variant::OneLock => "V1 one-lock ",
	}
}

fn dist_name(d: Dist) -> &'static str {
	match d {
		Dist::Unique => "unique",
		Dist::Hot => "hot   ",
	}
}

fn main() {
	const THREADS: &[u64] = &[1, 8, 32, 64, 128];
	const OBSERVED: f64 = 318_000.0; // 5M ops / 15.7s (Stage 1 Create rate)

	println!("\noracle critical-section ceiling  (TOTAL_OPS={TOTAL_OPS}, RETAIN={RETAIN})");
	println!("observed end-to-end Create rate ~= {OBSERVED:.0} commits/sec\n");
	println!("{:<12} {:<7} {:>4}  {:>14}  {:>9}  {:>10}", "variant", "dist", "N", "commits/sec", "x vs e2e", "conflicts");
	println!("{}", "-".repeat(64));

	for &dist in &[Dist::Unique, Dist::Hot] {
		for &variant in &[Variant::TwoLock, Variant::OneLock] {
			for &n in THREADS {
				let (rate, conflicts) = run(variant, dist, n);
				println!(
					"{:<12} {:<7} {:>4}  {:>14.0}  {:>8.1}x  {:>10}",
					variant_name(variant),
					dist_name(dist),
					n,
					rate,
					rate / OBSERVED,
					conflicts,
				);
			}
			println!();
		}
	}
}
