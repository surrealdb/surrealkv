//! Range scan throughput over a store shaped like a SurrealDB table after a bulk load.
//!
//! `cargo run --release --example scan_bench -- <dir> <records> [load]`
//! With `load`, the store is created and filled first. Every run then scans the whole range
//! once in one iterator, and once in chunks of 500 entries with a new iterator per chunk (as the
//! SurrealDB adapter reads).

use std::time::Instant;

use surrealkv::{LSMIterator, Mode, Options, TreeBuilder};

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

#[tokio::main(flavor = "multi_thread")]
async fn main() {
	let args: Vec<String> = std::env::args().collect();
	let dir = std::path::PathBuf::from(&args[1]);
	let records: u64 = args[2].parse().unwrap();
	let load = args.get(3).is_some_and(|a| a == "load" || a == "mem");
	// `mem`: a memtable large enough for every record, scanned while the records are in it.
	let mem = args.get(3).is_some_and(|a| a == "mem");
	let opts = Options::default()
		.with_path(dir.clone())
		.with_max_memtable_size(if mem { 2 << 30 } else { 64 << 20 })
		.with_target_file_size(16 << 20)
		.with_block_cache_capacity(16 << 30)
		.with_block_size(64 << 10)
		.with_vlog_value_threshold(4096);
	let tree = TreeBuilder::with_options(opts).build().unwrap();
	if load {
		let mut rng = fastrand::Rng::with_seed(7);
		let t = Instant::now();
		// Shuffled ids, as a bulk load of random record ids writes them.
		let mut ids: Vec<u64> = (0..records).collect();
		rng.shuffle(&mut ids);
		for chunk in ids.chunks(1000) {
			let mut tx = tree.begin().unwrap();
			for &i in chunk {
				tx.set(key(i), value(i, &mut rng)).unwrap();
			}
			tx.commit().await.unwrap();
		}
		eprintln!("loaded {records} in {:?}", t.elapsed());
		// Let the flushes and compactions settle.
		if !mem {
			tokio::time::sleep(std::time::Duration::from_secs(30)).await;
		}
	}
	let (start, end) = (key(0), key(u64::MAX / 2));
	let rounds: usize = std::env::var("ROUNDS").ok().and_then(|r| r.parse().ok()).unwrap_or(3);
	let (mut best_whole, mut best_chunked) = (f64::MAX, f64::MAX);
	for round in 0..rounds {
		let tx = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		let t = Instant::now();
		let mut n = 0u64;
		let mut bytes = 0u64;
		{
			let mut it = tx.range(start.as_slice(), end.as_slice()).unwrap();
			it.seek_first().unwrap();
			while it.valid() {
				bytes += it.value_ref().unwrap().len() as u64;
				n += 1;
				it.next().unwrap();
			}
		}
		let whole = t.elapsed();
		let t = Instant::now();
		let mut m = 0u64;
		let mut lo = start.clone();
		loop {
			let mut it = tx.range(lo.as_slice(), end.as_slice()).unwrap();
			it.seek_first().unwrap();
			let mut c = 0;
			while c < 500 && it.valid() {
				bytes += it.value_ref().unwrap().len() as u64;
				c += 1;
				it.next().unwrap();
			}
			m += c;
			if !it.valid() {
				break;
			}
			lo.clear();
			lo.extend_from_slice(it.key().user_key());
		}
		let chunked = t.elapsed();
		best_whole = best_whole.min(whole.as_nanos() as f64 / n as f64);
		best_chunked = best_chunked.min(chunked.as_nanos() as f64 / m as f64);
		eprintln!(
			"round {round}: whole {n} in {whole:?} ({:.0} ns/entry), chunked {m} in {chunked:?} ({:.0} ns/entry) [{bytes}]",
			whole.as_nanos() as f64 / n as f64,
			chunked.as_nanos() as f64 / m as f64
		);
	}
	eprintln!("best: whole {best_whole:.0} ns/entry, chunked {best_chunked:.0} ns/entry");
	tree.close().await.unwrap();
}
