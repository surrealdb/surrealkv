// Memtable-apply critical-section microbenchmark.
//
// The oracle and WAL are both proven NOT to be the wall on Linux (each sustains
// >1.4M commits/sec, vs the 318K/sec the engine delivers). ~65% of the
// per-commit budget is elsewhere; the cap-sweep signature (more concurrency =>
// SLOWER, fewer cores busy) points at a CONTENDED resource. The prime suspect
// is the memtable apply: every commit takes a read-guard on
// `active_memtable: Arc<RwLock<Arc<MemTable>>>` (std RwLock — a shared
// reader-count atom hit 128x) then does `add()` =
// `try_reserve` (CAS loop on one shared `reserved` atom) + a concurrent
// skiplist insert (arena bump + tower CAS).
//
// This bench drives the REAL `MemTable::add` path (via `bench_insert`) under an
// N-thread sweep, with unique keys (Create-like). Two variants isolate the
// lock from the structure:
//   V_apply : Arc<RwLock<Arc<MemTable>>> + read() + insert  (exact apply() path)
//   V_raw   : Arc<MemTable> + insert                        (skiplist/arena only)
// V_apply - V_raw = the std RwLock reader-count contention.
//
// Run: cargo bench --bench memtable_bench

use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::time::Instant;

use surrealkv::MemTable;

const TOTAL_OPS: u64 = 1_000_000;
const VAL_SIZE: usize = 64;
const OBSERVED: f64 = 318_000.0; // 5M ops / 15.7s (Stage 1 Create)

#[inline]
fn write_key(buf: &mut [u8; 26], mut n: u64) {
	for i in (0..26).rev() {
		buf[i] = b'0' + (n % 10) as u8;
		n /= 10;
	}
}

/// Generous arena so no rotation/ArenaFull happens mid-run (we measure steady
/// apply, not rotation).
fn new_memtable() -> MemTable {
	MemTable::new((TOTAL_OPS as usize) * 512)
}

/// V_apply: exact apply() path — read-guard on the RwLock, then insert.
fn run_apply(n_threads: u64) -> f64 {
	let mt = Arc::new(RwLock::new(Arc::new(new_memtable())));
	let barrier = Arc::new(Barrier::new(n_threads as usize + 1));
	let ops_per_thread = TOTAL_OPS / n_threads;

	let mut handles = Vec::with_capacity(n_threads as usize);
	for tid in 0..n_threads {
		let mt = Arc::clone(&mt);
		let barrier = Arc::clone(&barrier);
		handles.push(thread::spawn(move || {
			let val = vec![0xABu8; VAL_SIZE];
			let mut buf = [0u8; 26];
			let base = tid * ops_per_thread;
			barrier.wait();
			for i in 0..ops_per_thread {
				let id = base + i;
				write_key(&mut buf, id);
				let g = mt.read().unwrap();
				g.bench_insert(&buf, &val, id + 1).unwrap();
			}
		}));
	}
	barrier.wait();
	let t0 = Instant::now();
	for h in handles {
		h.join().unwrap();
	}
	let elapsed = t0.elapsed().as_secs_f64();
	(ops_per_thread * n_threads) as f64 / elapsed
}

/// V_raw: skiplist/arena only — no RwLock.
fn run_raw(n_threads: u64) -> f64 {
	let mt = Arc::new(new_memtable());
	let barrier = Arc::new(Barrier::new(n_threads as usize + 1));
	let ops_per_thread = TOTAL_OPS / n_threads;

	let mut handles = Vec::with_capacity(n_threads as usize);
	for tid in 0..n_threads {
		let mt = Arc::clone(&mt);
		let barrier = Arc::clone(&barrier);
		handles.push(thread::spawn(move || {
			let val = vec![0xABu8; VAL_SIZE];
			let mut buf = [0u8; 26];
			let base = tid * ops_per_thread;
			barrier.wait();
			for i in 0..ops_per_thread {
				let id = base + i;
				write_key(&mut buf, id);
				mt.bench_insert(&buf, &val, id + 1).unwrap();
			}
		}));
	}
	barrier.wait();
	let t0 = Instant::now();
	for h in handles {
		h.join().unwrap();
	}
	let elapsed = t0.elapsed().as_secs_f64();
	(ops_per_thread * n_threads) as f64 / elapsed
}

fn main() {
	println!("\nmemtable apply critical-section  (TOTAL_OPS={TOTAL_OPS}, VAL={VAL_SIZE}B, unique keys)");
	println!("reference: end-to-end Create ~= {OBSERVED:.0}/s (3.14us/commit)\n");

	println!("V_apply - exact apply() path (RwLock read-guard + add)");
	println!("{:<10} {:>14} {:>10} {:>10}", "N", "inserts/sec", "ns/insert", "x vs e2e");
	println!("{}", "-".repeat(48));
	for n in [1u64, 8, 32, 64, 128] {
		let rate = run_apply(n);
		println!("{:<10} {:>14.0} {:>10.0} {:>9.1}x", n, rate, 1e9 / rate, rate / OBSERVED);
	}

	println!("\nV_raw - skiplist/arena only (no RwLock)");
	println!("{:<10} {:>14} {:>10} {:>10}", "N", "inserts/sec", "ns/insert", "x vs e2e");
	println!("{}", "-".repeat(48));
	for n in [1u64, 8, 32, 64, 128] {
		let rate = run_raw(n);
		println!("{:<10} {:>14.0} {:>10.0} {:>9.1}x", n, rate, 1e9 / rate, rate / OBSERVED);
	}
	println!();
}
