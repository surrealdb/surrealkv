// WAL-append critical-section microbenchmark.
//
// In Stage 1 the WAL append runs INSIDE the commit `write_mutex`
// (`write_prepared` -> `wal.append` -> with manual_flush=false ->
// `BufWriter::flush()` = one write() syscall per commit). The oracle bench
// proved the oracle is NOT the wall (~446ns/commit vs the 3.14us end-to-end
// budget). This bench quantifies the WAL's share of the remaining ~2.7us and
// the upside of group commit (batching N appends behind ONE flush).
//
// It drives the REAL `WalWriter` (real CRC, fragmentation, block padding,
// file write()), to a fresh tmpdir per configuration.
//
// Part 1 - per-commit cost, single-thread serial ceiling:
//   V0       : flush() every commit            (== Stage 1, manual_flush=false)
//   VB(G)    : G appends + ONE flush per group  (group-commit amortization)
// Part 2 - contended V0 under a Mutex (models the single serialized WAL writer):
//   N in {1,8,32,64,128}
//
// Eventual durability => flush() to OS page cache, NO fsync (matches the
// benchmarked workload). Run: cargo bench --bench wal_bench

use std::fs::File;
use std::os::unix::fs::FileExt; // write_at == pwrite (positioned, thread-safe, no shared offset)
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Instant;

use surrealkv::{BufferedFileWriter, WalCompressionType, WalWriter, BLOCK_SIZE};
use tempfile::TempDir;

/// Commits per configuration. Smaller than the oracle bench to bound tmpdir
/// disk use (REC_SIZE * TOTAL_OPS bytes per config); still ample for stable timing.
const TOTAL_OPS: u64 = 1_000_000;
/// Representative encoded-record size (string26 key + small value + header).
const REC_SIZE: usize = 200;

const OBSERVED: f64 = 318_000.0; // 5M ops / 15.7s (Stage 1 Create)
const ORACLE_NS: f64 = 446.0; // oracle bench, N=1 per-section cost

fn new_writer(dir: &TempDir, manual_flush: bool) -> WalWriter {
	let file = File::create(dir.path().join("bench.wal")).unwrap();
	let bfw = BufferedFileWriter::new(file, BLOCK_SIZE);
	WalWriter::new(bfw, manual_flush, WalCompressionType::None, 0)
}

/// Part 1: single-thread serial ceiling. `group` = appends per flush (1 == V0).
fn run_serial(group: u64) -> f64 {
	let dir = TempDir::new().unwrap();
	let manual_flush = group > 1;
	let mut w = new_writer(&dir, manual_flush);
	let rec = vec![0xABu8; REC_SIZE];

	let t0 = Instant::now();
	let mut done = 0u64;
	while done < TOTAL_OPS {
		let g = group.min(TOTAL_OPS - done);
		for _ in 0..g {
			w.add_record(&rec).unwrap(); // manual_flush=false flushes here; =true buffers
		}
		if manual_flush {
			w.write_buffer().unwrap(); // one flush for the whole group
		}
		done += g;
	}
	let elapsed = t0.elapsed().as_secs_f64();
	TOTAL_OPS as f64 / elapsed
}

/// Part 2: N threads contending for one serialized WAL writer (flush/commit).
/// Each thread does a fixed share (no shared counter) so the loop always
/// terminates.
fn run_contended(n_threads: u64) -> f64 {
	let dir = TempDir::new().unwrap();
	let writer = Arc::new(Mutex::new(new_writer(&dir, false)));
	let barrier = Arc::new(Barrier::new(n_threads as usize + 1));
	let ops_per_thread = TOTAL_OPS / n_threads;

	let mut handles = Vec::with_capacity(n_threads as usize);
	for _ in 0..n_threads {
		let writer = Arc::clone(&writer);
		let barrier = Arc::clone(&barrier);
		handles.push(thread::spawn(move || {
			let rec = vec![0xABu8; REC_SIZE];
			barrier.wait();
			for _ in 0..ops_per_thread {
				let mut w = writer.lock().unwrap();
				w.add_record(&rec).unwrap();
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

/// Part 3: TideDB-style LOCK-FREE parallel WAL. Each writer atomically reserves
/// a file offset (`fetch_add`) and `pwrite`s a self-framed block (8-byte header
/// = len+crc, then data) at that offset — no lock, no leader/follower. File is
/// preallocated up front (like TideDB's `maybe_extend_allocation`) so pwrite
/// never serializes on file extension.
fn run_lockfree(n_threads: u64) -> f64 {
	const FRAME_HDR: u64 = 8; // u32 len + u32 crc
	let frame_len = FRAME_HDR + REC_SIZE as u64;

	let dir = TempDir::new().unwrap();
	let file = File::create(dir.path().join("bench.wal")).unwrap();
	file.set_len(TOTAL_OPS * frame_len).unwrap(); // preallocate
	let file = Arc::new(file);
	let offset = Arc::new(AtomicU64::new(0));
	let barrier = Arc::new(Barrier::new(n_threads as usize + 1));
	let ops_per_thread = TOTAL_OPS / n_threads;

	let mut handles = Vec::with_capacity(n_threads as usize);
	for _ in 0..n_threads {
		let file = Arc::clone(&file);
		let offset = Arc::clone(&offset);
		let barrier = Arc::clone(&barrier);
		handles.push(thread::spawn(move || {
			let data = vec![0xABu8; REC_SIZE];
			let mut frame = vec![0u8; frame_len as usize];
			frame[0..4].copy_from_slice(&(REC_SIZE as u32).to_le_bytes());
			frame[8..].copy_from_slice(&data);
			barrier.wait();
			for _ in 0..ops_per_thread {
				// Same per-record CPU as the real Writer (CRC over the payload).
				let crc = crc32fast::hash(&data);
				frame[4..8].copy_from_slice(&crc.to_le_bytes());
				let off = offset.fetch_add(frame_len, Ordering::Relaxed);
				file.write_at(&frame, off).unwrap();
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
	println!("\nWAL append critical-section  (TOTAL_OPS={TOTAL_OPS}, REC_SIZE={REC_SIZE}B, flush=write() to page cache, no fsync)");
	println!("reference: end-to-end Create ~= {OBSERVED:.0}/s (3.14us/commit); oracle section ~= {ORACLE_NS:.0}ns/commit\n");

	println!("Part 1 - single-thread serial ceiling");
	println!("{:<22} {:>14} {:>10} {:>10}", "variant", "commits/sec", "ns/commit", "x vs e2e");
	println!("{}", "-".repeat(58));
	for (label, group) in
		[("V0 flush/commit", 1u64), ("VB group=8", 8), ("VB group=32", 32), ("VB group=128", 128)]
	{
		let rate = run_serial(group);
		println!("{:<22} {:>14.0} {:>10.0} {:>9.1}x", label, rate, 1e9 / rate, rate / OBSERVED);
	}

	println!("\nPart 2 - contended V0 (flush/commit) under one serialized writer (surrealkv Stage 1 model)");
	println!("{:<22} {:>14} {:>10} {:>10}", "N threads", "commits/sec", "ns/commit", "x vs e2e");
	println!("{}", "-".repeat(58));
	for n in [1u64, 8, 32, 64, 128] {
		let rate = run_contended(n);
		println!("{:<22} {:>14.0} {:>10.0} {:>9.1}x", n, rate, 1e9 / rate, rate / OBSERVED);
	}

	println!("\nPart 3 - lock-free parallel pwrite, no coordination (TideDB model)");
	println!("{:<22} {:>14} {:>10} {:>10}", "N threads", "commits/sec", "ns/commit", "x vs e2e");
	println!("{}", "-".repeat(58));
	for n in [1u64, 8, 32, 64, 128] {
		let rate = run_lockfree(n);
		println!("{:<22} {:>14.0} {:>10.0} {:>9.1}x", n, rate, 1e9 / rate, rate / OBSERVED);
	}
	println!();
}
