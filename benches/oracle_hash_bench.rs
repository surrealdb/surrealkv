// Benchmark hashing strategies for `CommitOracle::recent_writes`.
//
// Two groups:
//
//   1. `hash_throughput` — pure cost of hashing N keys of fixed size, no map. Tells you how much
//      CPU the hash function itself costs.
//
//   2. `oracle_pattern` — end-to-end pattern the oracle actually runs: insert BATCH_SIZE (key, seq)
//      pairs, then look up BATCH_SIZE keys (half hits, half misses). Tells you what production will
//      feel.
//
// Strategies:
//
//   - siphash_fullkey      : HashMap<Vec<u8>, u64> with default SipHash      (current oracle impl)
//   - ahash_fullkey        : HashMap<Vec<u8>, u64> with ahash
//   - fxhash_fullkey       : HashMap<Vec<u8>, u64> with FxHash
//   - xxh3_fingerprint     : HashMap<u64,    u64> keyed by xxh3_64 fingerprint
//   - gxhash_fingerprint   : HashMap<u64,    u64> keyed by gxhash64 fingerprint
//   - ahash_fingerprint    : HashMap<u64,    u64> keyed by ahash(u64) fingerprint
//
// The "fingerprint" strategies pair with `nohash_hasher::IntMap` so the u64
// fingerprint is the final hash — no double-hashing.
//
// How to run:
//
//   cargo bench --bench oracle_hash_bench
//
// For honest numbers, force native target features (matters a lot for gxhash
// on x86 — AES-NI is gated; on Apple Silicon it's baseline so plain `cargo
// bench` is fine):
//
//   RUSTFLAGS="-C target-cpu=native" cargo bench --bench oracle_hash_bench
//
// Output lives at `target/criterion/<group>/<id>/report/index.html`.

use std::collections::HashMap;
use std::hash::{BuildHasher, BuildHasherDefault};

use ahash::RandomState as AHashState;
use criterion::{
	black_box,
	criterion_group,
	criterion_main,
	BatchSize,
	BenchmarkId,
	Criterion,
	Throughput,
};
use gxhash::gxhash64;
use nohash_hasher::BuildNoHashHasher;
use rustc_hash::FxHasher;
use xxhash_rust::xxh3::xxh3_64;

// Fixed seed so runs are reproducible across machines and reruns.
const SEED: u64 = 0xC0FFEE_C0FFEE;

// Spread of realistic SurrealKV key sizes.
const KEY_SIZES: [usize; 4] = [16, 32, 64, 128];

// Batch size per iteration. 1024 is realistic for "one oracle GC interval"
// worth of inserts and lookups.
const BATCH_SIZE: usize = 1024;

// ============================================================
// Helpers
// ============================================================

fn gen_keys(n: usize, size: usize) -> Vec<Vec<u8>> {
	let mut rng = fastrand::Rng::with_seed(SEED);
	(0..n)
		.map(|_| {
			let mut k = vec![0u8; size];
			for byte in &mut k {
				*byte = rng.u8(..);
			}
			k
		})
		.collect()
}

/// Build the lookup workload: half existing keys (hits), half fresh keys (misses).
fn gen_lookups(existing: &[Vec<u8>], size: usize) -> Vec<Vec<u8>> {
	let half = existing.len() / 2;
	let mut rng = fastrand::Rng::with_seed(SEED ^ 0xDEADBEEF);
	let mut out = Vec::with_capacity(existing.len());
	out.extend(existing.iter().take(half).cloned());
	for _ in 0..(existing.len() - half) {
		let mut k = vec![0u8; size];
		for byte in &mut k {
			*byte = rng.u8(..);
		}
		out.push(k);
	}
	out
}

// ============================================================
// Group 1: pure hash throughput
// ============================================================

fn bench_hash_throughput(c: &mut Criterion) {
	let mut g = c.benchmark_group("hash_throughput");

	for &size in &KEY_SIZES {
		let keys = gen_keys(BATCH_SIZE, size);
		g.throughput(Throughput::Bytes((size * BATCH_SIZE) as u64));

		g.bench_with_input(BenchmarkId::new("xxh3_64", size), &keys, |b, ks| {
			b.iter(|| {
				let mut acc: u64 = 0;
				for k in ks {
					acc = acc.wrapping_add(xxh3_64(black_box(k)));
				}
				acc
			})
		});

		g.bench_with_input(BenchmarkId::new("gxhash64", size), &keys, |b, ks| {
			b.iter(|| {
				let mut acc: u64 = 0;
				for k in ks {
					acc = acc.wrapping_add(gxhash64(black_box(k), 0));
				}
				acc
			})
		});

		g.bench_with_input(BenchmarkId::new("ahash", size), &keys, |b, ks| {
			let state = AHashState::with_seeds(1, 2, 3, 4);
			b.iter(|| {
				let mut acc: u64 = 0;
				for k in ks {
					acc = acc.wrapping_add(state.hash_one(black_box(k.as_slice())));
				}
				acc
			})
		});

		g.bench_with_input(BenchmarkId::new("fxhash", size), &keys, |b, ks| {
			b.iter(|| {
				let mut acc: u64 = 0;
				for k in ks {
					let mut h = FxHasher::default();
					std::hash::Hasher::write(&mut h, black_box(k));
					acc = acc.wrapping_add(std::hash::Hasher::finish(&h));
				}
				acc
			})
		});

		g.bench_with_input(BenchmarkId::new("siphash", size), &keys, |b, ks| {
			let state = std::collections::hash_map::RandomState::new();
			b.iter(|| {
				let mut acc: u64 = 0;
				for k in ks {
					acc = acc.wrapping_add(state.hash_one(black_box(k.as_slice())));
				}
				acc
			})
		});
	}
	g.finish();
}

// ============================================================
// Group 2: oracle access pattern (insert + lookup)
// ============================================================

type IntMap = HashMap<u64, u64, BuildNoHashHasher<u64>>;
type FxByteMap = HashMap<Vec<u8>, u64, BuildHasherDefault<FxHasher>>;
type AHashByteMap = HashMap<Vec<u8>, u64, AHashState>;
type SipByteMap = HashMap<Vec<u8>, u64>;

fn bench_oracle_pattern(c: &mut Criterion) {
	let mut g = c.benchmark_group("oracle_pattern");

	for &size in &KEY_SIZES {
		let keys = gen_keys(BATCH_SIZE, size);
		let lookups = gen_lookups(&keys, size);

		// One "iteration" = insert BATCH_SIZE + lookup BATCH_SIZE = 2 * BATCH_SIZE ops.
		g.throughput(Throughput::Elements((BATCH_SIZE * 2) as u64));

		// --- Strategy 1: SipHash + full-key map (matches current oracle.rs) ---
		g.bench_with_input(
			BenchmarkId::new("siphash_fullkey", size),
			&(&keys, &lookups),
			|b, (ks, ls)| {
				b.iter_batched(
					|| SipByteMap::with_capacity(BATCH_SIZE),
					|mut map| {
						for (i, k) in ks.iter().enumerate() {
							map.insert(k.clone(), i as u64);
						}
						let mut acc: u64 = 0;
						for k in ls.iter() {
							if let Some(&v) = map.get(black_box(k.as_slice())) {
								acc = acc.wrapping_add(v);
							}
						}
						acc
					},
					BatchSize::SmallInput,
				)
			},
		);

		// --- Strategy 2: ahash + full-key map ---
		g.bench_with_input(
			BenchmarkId::new("ahash_fullkey", size),
			&(&keys, &lookups),
			|b, (ks, ls)| {
				b.iter_batched(
					|| {
						AHashByteMap::with_capacity_and_hasher(
							BATCH_SIZE,
							AHashState::with_seeds(1, 2, 3, 4),
						)
					},
					|mut map| {
						for (i, k) in ks.iter().enumerate() {
							map.insert(k.clone(), i as u64);
						}
						let mut acc: u64 = 0;
						for k in ls.iter() {
							if let Some(&v) = map.get(black_box(k.as_slice())) {
								acc = acc.wrapping_add(v);
							}
						}
						acc
					},
					BatchSize::SmallInput,
				)
			},
		);

		// --- Strategy 3: FxHash + full-key map ---
		g.bench_with_input(
			BenchmarkId::new("fxhash_fullkey", size),
			&(&keys, &lookups),
			|b, (ks, ls)| {
				b.iter_batched(
					|| FxByteMap::with_capacity_and_hasher(BATCH_SIZE, Default::default()),
					|mut map| {
						for (i, k) in ks.iter().enumerate() {
							map.insert(k.clone(), i as u64);
						}
						let mut acc: u64 = 0;
						for k in ls.iter() {
							if let Some(&v) = map.get(black_box(k.as_slice())) {
								acc = acc.wrapping_add(v);
							}
						}
						acc
					},
					BatchSize::SmallInput,
				)
			},
		);

		// --- Strategy 4: xxh3_64 fingerprint + IntMap ---
		g.bench_with_input(
			BenchmarkId::new("xxh3_fingerprint", size),
			&(&keys, &lookups),
			|b, (ks, ls)| {
				b.iter_batched(
					|| IntMap::with_capacity_and_hasher(BATCH_SIZE, BuildNoHashHasher::default()),
					|mut map| {
						for (i, k) in ks.iter().enumerate() {
							map.insert(xxh3_64(k), i as u64);
						}
						let mut acc: u64 = 0;
						for k in ls.iter() {
							let fp = xxh3_64(black_box(k));
							if let Some(&v) = map.get(&fp) {
								acc = acc.wrapping_add(v);
							}
						}
						acc
					},
					BatchSize::SmallInput,
				)
			},
		);

		// --- Strategy 5: gxhash fingerprint + IntMap ---
		g.bench_with_input(
			BenchmarkId::new("gxhash_fingerprint", size),
			&(&keys, &lookups),
			|b, (ks, ls)| {
				b.iter_batched(
					|| IntMap::with_capacity_and_hasher(BATCH_SIZE, BuildNoHashHasher::default()),
					|mut map| {
						for (i, k) in ks.iter().enumerate() {
							map.insert(gxhash64(k, 0), i as u64);
						}
						let mut acc: u64 = 0;
						for k in ls.iter() {
							let fp = gxhash64(black_box(k), 0);
							if let Some(&v) = map.get(&fp) {
								acc = acc.wrapping_add(v);
							}
						}
						acc
					},
					BatchSize::SmallInput,
				)
			},
		);

		// --- Strategy 6: ahash fingerprint + IntMap ---
		g.bench_with_input(
			BenchmarkId::new("ahash_fingerprint", size),
			&(&keys, &lookups),
			|b, (ks, ls)| {
				let state = AHashState::with_seeds(1, 2, 3, 4);
				b.iter_batched(
					|| IntMap::with_capacity_and_hasher(BATCH_SIZE, BuildNoHashHasher::default()),
					|mut map| {
						for (i, k) in ks.iter().enumerate() {
							map.insert(state.hash_one(k.as_slice()), i as u64);
						}
						let mut acc: u64 = 0;
						for k in ls.iter() {
							let fp = state.hash_one(black_box(k.as_slice()));
							if let Some(&v) = map.get(&fp) {
								acc = acc.wrapping_add(v);
							}
						}
						acc
					},
					BatchSize::SmallInput,
				)
			},
		);
	}
	g.finish();
}

criterion_group!(benches, bench_hash_throughput, bench_oracle_pattern);
criterion_main!(benches);
