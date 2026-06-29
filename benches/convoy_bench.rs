// Commit-pipeline convoy microbench.
//
// off-CPU profiling proved the write wall is the global synchronous
// `write_mutex`, taken once per commit: committers block on each other's
// lock and the per-commit cost is dominated by the lock HANDOFF (futex wake +
// reschedule), which is OS-specific. This bench abstracts that: a shared mutex,
// a tunable amount of in-lock "work", and a grouping knob (commits per lock
// trip). It answers the design question on the TARGET hardware:
//
//   * Is throughput handoff-dominated? -> compare work=0 vs work=W at group=1.
//     If work=0 is barely faster than work=W, the handoff is the floor and only
//     FEWER LOCK TRIPS (group commit) can help.
//   * How much does GROUPING buy? -> group=1 vs 16 vs 64 (one trip per G items).
//   * Does the lock impl matter? -> parking_lot vs std::sync::Mutex.
//
// MUST be run on the 64-core Linux target (lock-handoff latency differs by OS).
// Run: cargo bench --bench convoy_bench

use std::sync::{Arc, Barrier, Mutex as StdMutex};
use std::thread;
use std::time::Instant;

use parking_lot::Mutex as PlMutex;

const TOTAL_OPS: u64 = 2_000_000;
const OBSERVED: f64 = 344_000.0; // current surrealkv Create rate (5M/14.5s)
const ROCKSDB: f64 = 432_000.0; // rocksdb Create rate (1M/2.317s)

/// Simulated in-lock work (oracle HashMap + seq + enqueue ~ a few hundred ns).
/// Tuned by iteration count; relative comparisons are what matter.
#[inline]
fn work(iters: u32) {
	let mut x = 0u64;
	for i in 0..iters {
		x = x.wrapping_add(i as u64).wrapping_mul(2_654_435_761);
	}
	std::hint::black_box(x);
}

fn run_pl(n_threads: u64, work_iters: u32, group: u64) -> f64 {
	let m = Arc::new(PlMutex::new(0u64));
	let barrier = Arc::new(Barrier::new(n_threads as usize + 1));
	let per = TOTAL_OPS / n_threads;
	let mut handles = Vec::new();
	for _ in 0..n_threads {
		let m = Arc::clone(&m);
		let barrier = Arc::clone(&barrier);
		handles.push(thread::spawn(move || {
			barrier.wait();
			let mut i = 0;
			while i < per {
				let chunk = group.min(per - i);
				let mut g = m.lock();
				for _ in 0..chunk {
					work(work_iters);
					*g += 1;
				}
				drop(g);
				i += chunk;
			}
		}));
	}
	barrier.wait();
	let t0 = Instant::now();
	for h in handles {
		h.join().unwrap();
	}
	(per * n_threads) as f64 / t0.elapsed().as_secs_f64()
}

fn run_std(n_threads: u64, work_iters: u32, group: u64) -> f64 {
	let m = Arc::new(StdMutex::new(0u64));
	let barrier = Arc::new(Barrier::new(n_threads as usize + 1));
	let per = TOTAL_OPS / n_threads;
	let mut handles = Vec::new();
	for _ in 0..n_threads {
		let m = Arc::clone(&m);
		let barrier = Arc::clone(&barrier);
		handles.push(thread::spawn(move || {
			barrier.wait();
			let mut i = 0;
			while i < per {
				let chunk = group.min(per - i);
				let mut g = m.lock().unwrap();
				for _ in 0..chunk {
					work(work_iters);
					*g += 1;
				}
				drop(g);
				i += chunk;
			}
		}));
	}
	barrier.wait();
	let t0 = Instant::now();
	for h in handles {
		h.join().unwrap();
	}
	(per * n_threads) as f64 / t0.elapsed().as_secs_f64()
}

fn row(label: &str, n: u64, rate: f64) {
	println!(
		"{:<34}{:>4}  {:>13.0} {:>9.0}  {:>6.2}x e2e {:>6.2}x rocks",
		label,
		n,
		rate,
		1e9 / rate,
		rate / OBSERVED,
		rate / ROCKSDB,
	);
}

fn main() {
	// Calibrate: ~300 iters of mul/add ~ a few hundred ns (the rough in-lock work).
	const W: u32 = 300;
	println!("\ncommit-pipeline convoy: shared mutex, tunable in-lock work, grouping knob (TOTAL_OPS={TOTAL_OPS})");
	println!("ref: surrealkv Create ~={OBSERVED:.0}/s, rocksdb ~={ROCKSDB:.0}/s\n");
	println!("{:<34}{:>4}  {:>13} {:>9}  {:>11} {:>11}", "variant", "N", "ops/sec", "ns/op", "", "");
	println!("{}", "-".repeat(92));

	for n in [7u64, 32, 64, 128] {
		row("pl  work=W   group=1  (baseline)", n, run_pl(n, W, 1));
	}
	println!();
	for n in [7u64, 32, 64, 128] {
		row("pl  work=0   group=1  (handoff floor)", n, run_pl(n, 0, 1));
	}
	println!();
	for n in [7u64, 32, 64, 128] {
		row("pl  work=W   group=16 (group commit)", n, run_pl(n, W, 16));
	}
	println!();
	for n in [7u64, 32, 64, 128] {
		row("pl  work=W   group=64 (big group)", n, run_pl(n, W, 64));
	}
	println!();
	for n in [7u64, 32, 64, 128] {
		row("std work=W   group=1  (std mutex)", n, run_std(n, W, 1));
	}
	println!();
}
