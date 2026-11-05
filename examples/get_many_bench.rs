//! Point reads of a batch of keys, kept or rejected by a test of the value, as an index scan
//! with a secondary filter reads the records its index hits name.
//!
//! `cargo run --release --example get_many_bench -- <dir> <records> <mode> [load]`
//! `mode` is `owned` (`get_many`, then the test on the copies) or `borrowed` (`get_many_with`,
//! the test on the borrowed bytes, and a copy only of what is kept). One record in `KEEP_EVERY`
//! (default 12) is kept. With `load`, the store is created and filled first. `THREADS` (48),
//! `BATCH` (100) and `BATCHES` (per thread, 2000) set the work.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use surrealkv::{Options, TreeBuilder};

fn key(i: u64) -> Vec<u8> {
	let mut k = b"/*test\0*test\0*record\0*".to_vec();
	k.extend_from_slice(format!("{i:020}").as_bytes());
	k
}

fn value(i: u64, rng: &mut fastrand::Rng) -> Vec<u8> {
	let len = 1000 + rng.usize(0..800);
	let mut v = Vec::with_capacity(len);
	v.extend_from_slice(&i.to_le_bytes());
	while v.len() < len {
		v.push(b'a' + rng.u8(0..26));
	}
	v
}

fn env(name: &str, default: usize) -> usize {
	std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
	let args: Vec<String> = std::env::args().collect();
	let dir = std::path::PathBuf::from(&args[1]);
	let records: u64 = args[2].parse().unwrap();
	let borrowed = args[3] == "borrowed";
	let load = args.get(4).is_some_and(|a| a == "load");
	let opts = Options::default()
		.with_path(dir)
		.with_max_memtable_size(64 << 20)
		.with_target_file_size(16 << 20)
		.with_block_cache_capacity(16 << 30)
		.with_block_size(64 << 10)
		.with_vlog_value_threshold(4096);
	let tree = std::sync::Arc::new(TreeBuilder::with_options(opts).build().unwrap());
	if load {
		let mut rng = fastrand::Rng::with_seed(7);
		let mut ids: Vec<u64> = (0..records).collect();
		rng.shuffle(&mut ids);
		for chunk in ids.chunks(1000) {
			let mut tx = tree.begin().unwrap();
			for &i in chunk {
				tx.set(key(i), value(i, &mut rng)).unwrap();
			}
			tx.commit().await.unwrap();
		}
		tokio::time::sleep(std::time::Duration::from_secs(30)).await;
		eprintln!("loaded {records}");
		return;
	}
	let (threads, batch, batches) = (env("THREADS", 48), env("BATCH", 100), env("BATCHES", 2000));
	let keep_every = env("KEEP_EVERY", 12) as u64;
	let kept = AtomicU64::new(0);
	let bytes = AtomicU64::new(0);
	let distinct = env("DISTINCT", 200);
	let ready = std::sync::Barrier::new(threads + 1);
	let mut t = Instant::now();
	std::thread::scope(|scope| {
		for n in 0..threads {
			let (tree, kept, bytes, ready) = (&tree, &kept, &bytes, &ready);
			scope.spawn(move || {
				let mut rng = fastrand::Rng::with_seed(n as u64);
				let mut local = (0u64, 0u64);
				// The keys are made before the timed window and reused in turn: making them
				// is a lot of small allocations, which would be most of what is measured
				let sets: Vec<Vec<Vec<u8>>> = (0..distinct)
					.map(|_| (0..batch).map(|_| key(rng.u64(0..records))).collect())
					.collect();
				ready.wait();
				for round in 0..batches {
					let keys = &sets[round % distinct];
					let tx = tree.begin().unwrap();
					// A record is kept when its id, stored in its first bytes, says so
					let keep =
						|v: &[u8]| u64::from_le_bytes(v[..8].try_into().unwrap()) % keep_every == 0;
					if borrowed {
						let mut kept_values: Vec<Vec<u8>> = Vec::new();
						tx.get_many_with(keys, |_, v| {
							if let Some(v) = v.filter(|v| keep(v)) {
								kept_values.push(v.to_vec());
							}
						})
						.unwrap();
						local.0 += kept_values.len() as u64;
						local.1 += kept_values.iter().map(|v| v.len() as u64).sum::<u64>();
					} else {
						let values = tx.get_many(keys).unwrap();
						for v in values.into_iter().flatten().filter(|v| keep(v)) {
							local.0 += 1;
							local.1 += v.len() as u64;
						}
					}
				}
				kept.fetch_add(local.0, Ordering::Relaxed);
				bytes.fetch_add(local.1, Ordering::Relaxed);
			});
		}
		// The clock starts when every thread has its keys
		ready.wait();
		t = Instant::now();
	});
	let wall = t.elapsed();
	let reads = (threads * batch * batches) as f64;
	println!(
		"{} wall={:.3}s reads/s={:.0} kept={} kept_bytes={}",
		args[3],
		wall.as_secs_f64(),
		reads / wall.as_secs_f64(),
		kept.load(Ordering::Relaxed),
		bytes.load(Ordering::Relaxed)
	);
}
