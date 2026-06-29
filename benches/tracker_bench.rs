// MVCC active-set tracker microbenchmark (confirmation gate for the
// SkipSet -> ShardedMinTracker replacement).
//
// Reproduces the per-transaction churn that a crud-bench flamegraph showed
// burning ~40% of CPU in crossbeam_skiplist: each "transaction" registers a
// seq, reads the oldest (the per-commit GC watermark), then unregisters —
// hammering insert/remove/front on the shared structure.
//
//   CURRENT : crossbeam_skiplist::SkipSet<(seq, unique_id)>  (== ActiveTxnTracker)
//   NEW     : surrealkv::ShardedMinTracker
//
// The new design must (a) crush crossbeam at high N and (b) keep SCALING with
// cores, NOT plateau the way a single global mutex does (WAL bench Part 2).
//
// Run: cargo bench --bench tracker_bench

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use crossbeam_skiplist::SkipSet;
use surrealkv::ShardedMinTracker;

const TOTAL_OPS: u64 = 2_000_000; // register+oldest+unregister cycles, split across threads
const OBSERVED: f64 = 318_000.0; // engine's end-to-end Create rate, for scale

/// Baseline mirroring the current `ActiveTxnTracker`: a global lock-free
/// `SkipSet<(start_seq, unique_id)>`, `front()` for the oldest.
struct CrossbeamBaseline {
	seqs: SkipSet<(u64, u64)>,
	next_id: AtomicU64,
}
impl CrossbeamBaseline {
	fn new() -> Self {
		Self {
			seqs: SkipSet::new(),
			next_id: AtomicU64::new(0),
		}
	}
	fn register(&self, seq: u64) -> (u64, u64) {
		let id = self.next_id.fetch_add(1, Ordering::Relaxed);
		let e = (seq, id);
		self.seqs.insert(e);
		e
	}
	fn unregister(&self, e: &(u64, u64)) {
		self.seqs.remove(e);
	}
	fn oldest(&self) -> Option<u64> {
		self.seqs.front().map(|x| x.value().0)
	}
}

fn run_crossbeam(n_threads: u64) -> f64 {
	let t = Arc::new(CrossbeamBaseline::new());
	let barrier = Arc::new(Barrier::new(n_threads as usize + 1));
	let per = TOTAL_OPS / n_threads;
	let mut handles = Vec::new();
	for tid in 0..n_threads {
		let t = Arc::clone(&t);
		let barrier = Arc::clone(&barrier);
		handles.push(thread::spawn(move || {
			let base = tid * per;
			barrier.wait();
			for i in 0..per {
				let seq = base + i;
				let e = t.register(seq);
				let _ = t.oldest();
				t.unregister(&e);
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

fn run_sharded(n_threads: u64) -> f64 {
	let t = Arc::new(ShardedMinTracker::new());
	let barrier = Arc::new(Barrier::new(n_threads as usize + 1));
	let per = TOTAL_OPS / n_threads;
	let mut handles = Vec::new();
	for tid in 0..n_threads {
		let t = Arc::clone(&t);
		let barrier = Arc::clone(&barrier);
		handles.push(thread::spawn(move || {
			let base = tid * per;
			barrier.wait();
			for i in 0..per {
				let seq = base + i;
				let shard = t.register(seq);
				let _ = t.oldest();
				t.unregister(shard, seq);
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

fn main() {
	println!("\nMVCC active-set tracker: register + oldest + unregister per op  (TOTAL_OPS={TOTAL_OPS})");
	println!("reference: engine end-to-end Create ~= {OBSERVED:.0}/s\n");

	println!("{:<22} {:>14} {:>10} {:>10}", "impl / N", "ops/sec", "ns/op", "x vs e2e");
	println!("{}", "-".repeat(60));
	for (label, f) in [
		("CURRENT crossbeam", run_crossbeam as fn(u64) -> f64),
		("NEW sharded", run_sharded as fn(u64) -> f64),
	] {
		for n in [1u64, 8, 32, 64, 128] {
			let rate = f(n);
			println!("{:<14}{:>4}    {:>14.0} {:>10.0} {:>9.1}x", label, n, rate, 1e9 / rate, rate / OBSERVED);
		}
		println!();
	}
}
