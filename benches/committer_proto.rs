// Prototype benchmark for the single-commit-thread pipeline (the GATE).
//
// Faithfully models the proposed design BEFORE implementing it, driving the
// REAL components (CommitOracle, WalWriter, MemTable): one dedicated commit
// thread owns seq/oracle/WAL/memtable/visible_seq; submitters push a request
// over an mpsc channel and wait; the commit thread drains whatever is queued as
// a GROUP, processes it serially (oracle.check -> seq -> oracle.publish ->
// batched WAL append+flush -> memtable apply -> advance visible_seq), then
// completes the group. std::sync::mpsc gives the commit-thread park/wake for
// free (recv blocks when idle, send wakes it); no new deps.
//
// The completion protocol is RocksDB-AwaitState-style, parameterized by a spin
// count so we can isolate the cost of the wake SYSCALL:
//   state INIT -> (committer spins SPIN times checking DONE) -> if not done, CAS
//   to PARKED and park. Commit thread: swap(DONE); unpark ONLY if prev==PARKED.
//   SPIN=0  => committer always parks, commit thread always unparks (1 futex
//              wake syscall PER COMMIT, serialized on the commit thread).
//   SPIN=N  => committer that catches DONE within the spin window costs the
//              commit thread NO syscall (the prime hope for relieving the
//              wake-bound ceiling).
//
// Variants attribute the ceiling without bias:
//   full       : oracle + batched WAL + serial apply  (the design)
//   coord-only : submit + complete, NO work           (the wake-path ceiling)
//   no-apply / no-wal : isolate the binding component
//
// MUST be run on the 64-core Linux target (park/unpark + syscall costs are
// OS-specific; Mac park/unpark is much slower than Linux futex). Compare to
// 344K (current pipeline) and 432-500K (rocksdb). avg group size proves batching.
//
// Run: cargo bench --bench committer_proto

use std::fs::File;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread::{self, Thread};
use std::time::Instant;

use surrealkv::{BufferedFileWriter, CommitOracle, MemTable, WalCompressionType, WalWriter, BLOCK_SIZE};
use tempfile::TempDir;

const TOTAL_OPS: u64 = 1_000_000;
const REC_SIZE: usize = 200;
const VAL_SIZE: usize = 64;
const ARENA: usize = 256 * 1024 * 1024;
const RETAIN: u64 = 100_000;
const OBSERVED: f64 = 344_000.0;

const ST_INIT: u8 = 0;
const ST_PARKED: u8 = 1;
const ST_DONE: u8 = 2;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
	Full,
	CoordOnly,
	NoApply,
	NoWal,
}
impl Mode {
	fn uses_oracle(self) -> bool {
		self != Mode::CoordOnly
	}
	fn uses_wal(self) -> bool {
		self == Mode::Full || self == Mode::NoApply
	}
	fn uses_apply(self) -> bool {
		self == Mode::Full || self == Mode::NoWal
	}
	fn name(self) -> &'static str {
		match self {
			Mode::Full => "full(oracle+wal+apply)",
			Mode::CoordOnly => "coord-only",
			Mode::NoApply => "no-apply(oracle+wal)",
			Mode::NoWal => "no-wal(oracle+apply)",
		}
	}
}

struct Completion {
	state: AtomicU8,
	waiter: Thread,
}

struct Req {
	key: [u8; 26],
	val: Vec<u8>,
	wal: Vec<u8>,
	start_seq: u64,
	seq: u64,
	comp: Arc<Completion>,
}

#[inline]
fn write_key(buf: &mut [u8; 26], mut n: u64) {
	for i in (0..26).rev() {
		buf[i] = b'0' + (n % 10) as u8;
		n /= 10;
	}
}

fn run(n_threads: u64, mode: Mode, spin: u32) -> (f64, f64) {
	let dir = TempDir::new().unwrap();
	let wal_path = dir.path().join("proto.wal");
	let visible = Arc::new(AtomicU64::new(0));
	let (tx, rx) = mpsc::channel::<Req>();
	let per = TOTAL_OPS / n_threads;
	let total = per * n_threads;

	let visible_c = Arc::clone(&visible);
	let commit = thread::spawn(move || {
		let oracle = CommitOracle::new();
		let mut seq: u64 = 1;
		let mut groups: u64 = 0;
		let mut processed: u64 = 0;
		let mut wal = if mode.uses_wal() {
			let f = File::create(&wal_path).unwrap();
			Some(WalWriter::new(BufferedFileWriter::new(f, BLOCK_SIZE), true, WalCompressionType::None, 0))
		} else {
			None
		};
		let mut mt = if mode.uses_apply() {
			Some(MemTable::new(ARENA))
		} else {
			None
		};

		loop {
			let first = match rx.recv() {
				Ok(r) => r,
				Err(_) => break,
			};
			let mut group = vec![first];
			while let Ok(r) = rx.try_recv() {
				group.push(r);
			}
			groups += 1;
			processed += group.len() as u64;

			for req in group.iter_mut() {
				if mode.uses_oracle() {
					let _ = oracle.check([req.key.as_slice()], req.start_seq);
				}
				req.seq = seq;
				seq += 1;
				if mode.uses_oracle() {
					let oldest = req.seq.saturating_sub(RETAIN);
					oracle.publish([req.key.as_slice()], req.seq, 1, oldest);
				}
				if mode.uses_wal() {
					req.wal[0..8].copy_from_slice(&req.seq.to_le_bytes());
				}
			}

			if let Some(w) = wal.as_mut() {
				for req in &group {
					w.add_record(&req.wal).unwrap();
				}
				w.write_buffer().unwrap();
			}

			if mode.uses_apply() {
				for req in &group {
					if mt.as_ref().unwrap().bench_insert(&req.key, &req.val, req.seq).is_err() {
						mt = Some(MemTable::new(ARENA));
						mt.as_ref().unwrap().bench_insert(&req.key, &req.val, req.seq).unwrap();
					}
				}
			}

			if let Some(last) = group.last() {
				visible_c.store(last.seq, Ordering::Release);
			}

			// Complete: mark DONE; only pay an unpark() syscall if the committer
			// had already parked (spin window missed).
			for req in &group {
				let prev = req.comp.state.swap(ST_DONE, Ordering::Release);
				if prev == ST_PARKED {
					req.comp.waiter.unpark();
				}
			}
		}
		(groups, processed)
	});

	let barrier = Arc::new(Barrier::new(n_threads as usize + 1));
	let mut subs = Vec::new();
	for tid in 0..n_threads {
		let tx = tx.clone();
		let barrier = Arc::clone(&barrier);
		let visible = Arc::clone(&visible);
		subs.push(thread::spawn(move || {
			let me = thread::current();
			let mut key = [0u8; 26];
			barrier.wait();
			for i in 0..per {
				write_key(&mut key, tid * per + i);
				let comp = Arc::new(Completion {
					state: AtomicU8::new(ST_INIT),
					waiter: me.clone(),
				});
				tx.send(Req {
					key,
					val: vec![0xABu8; VAL_SIZE],
					wal: vec![0xCDu8; REC_SIZE],
					start_seq: visible.load(Ordering::Relaxed),
					seq: 0,
					comp: Arc::clone(&comp),
				})
				.unwrap();

				// spin window
				let mut done = false;
				for _ in 0..spin {
					if comp.state.load(Ordering::Acquire) == ST_DONE {
						done = true;
						break;
					}
					std::hint::spin_loop();
				}
				if !done {
					// transition INIT -> PARKED (unless already DONE), then park.
					if comp
						.state
						.compare_exchange(ST_INIT, ST_PARKED, Ordering::AcqRel, Ordering::Acquire)
						.is_ok()
					{
						while comp.state.load(Ordering::Acquire) != ST_DONE {
							thread::park();
						}
					}
					// else: commit thread already set DONE; nothing to wait for.
				}
			}
		}));
	}
	drop(tx);

	barrier.wait();
	let t0 = Instant::now();
	for s in subs {
		s.join().unwrap();
	}
	let elapsed = t0.elapsed().as_secs_f64();
	let (groups, processed) = commit.join().unwrap();
	(total as f64 / elapsed, processed as f64 / groups.max(1) as f64)
}

fn row(label: &str, n: u64, rate: f64, avg_group: f64) {
	println!(
		"{:<30}{:>4}  {:>13.0} {:>9.0}  {:>10.1} {:>8.2}x",
		label,
		n,
		rate,
		1e9 / rate,
		avg_group,
		rate / OBSERVED,
	);
}

fn main() {
	println!("\nsingle-commit-thread prototype (TOTAL_OPS={TOTAL_OPS}, REC={REC_SIZE}B, VAL={VAL_SIZE}B)");
	println!("ref: current pipeline ~={OBSERVED:.0}/s, rocksdb ~=432000-500000/s\n");
	println!("{:<30}{:>4}  {:>13} {:>9}  {:>10} {:>8}", "variant", "N", "ops/sec", "ns/op", "avg group", "x e2e");
	println!("{}", "-".repeat(82));

	// (mode, spin, label)
	let configs: &[(Mode, u32, &str)] = &[
		(Mode::Full, 0, "full   park (spin=0)"),
		(Mode::Full, 4096, "full   spin=4096"),
		(Mode::CoordOnly, 0, "coord  park (spin=0)"),
		(Mode::CoordOnly, 4096, "coord  spin=4096"),
		(Mode::NoApply, 0, "no-apply park"),
		(Mode::NoWal, 0, "no-wal park"),
	];
	for (mode, spin, label) in configs {
		for n in [7u64, 32, 64, 128] {
			let (rate, avg_group) = run(n, *mode, *spin);
			row(label, n, rate, avg_group);
		}
		println!();
	}
}
