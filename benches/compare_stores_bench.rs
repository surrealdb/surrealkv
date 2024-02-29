// This code is borrowed from: https://github.com/cberner/redb/blob/master/benches/lmdb_benchmark.rs
//
// Copyright (c) 2021 Christopher Berner
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use fastrand::Rng;
use std::mem::size_of;
use std::sync::Arc;
use std::thread;
use tempdir::TempDir;

mod common;
use common::*;

use std::fs::File;
use std::time::{Duration, Instant};

const ELEMENTS: usize = 1_000_00;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const RNG_SEED: u64 = 3;

fn create_temp_directory() -> TempDir {
    TempDir::new("test").unwrap()
}

fn fill_slice(slice: &mut [u8], rng: &mut fastrand::Rng) {
    let mut i = 0;
    while i + size_of::<u128>() < slice.len() {
        let tmp = rng.u128(..);
        slice[i..(i + size_of::<u128>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u128>()
    }
    if i + size_of::<u64>() < slice.len() {
        let tmp = rng.u64(..);
        slice[i..(i + size_of::<u64>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u64>()
    }
    if i + size_of::<u32>() < slice.len() {
        let tmp = rng.u32(..);
        slice[i..(i + size_of::<u32>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u32>()
    }
    if i + size_of::<u16>() < slice.len() {
        let tmp = rng.u16(..);
        slice[i..(i + size_of::<u16>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u16>()
    }
    if i + size_of::<u8>() < slice.len() {
        slice[i] = rng.u8(..);
    }
}

/// Returns pairs of key, value
fn gen_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
    let mut key = [0u8; KEY_SIZE];
    fill_slice(&mut key, rng);
    let mut value = vec![0u8; VALUE_SIZE];
    fill_slice(&mut value, rng);

    (key, value)
}

fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
}

fn make_rng_shards(shards: usize, elements: usize) -> Vec<fastrand::Rng> {
    let mut rngs = vec![];
    let elements_per_shard = elements / shards;
    for i in 0..shards {
        let mut rng = make_rng();
        for _ in 0..(i * elements_per_shard) {
            gen_pair(&mut rng);
        }
        rngs.push(rng);
    }

    rngs
}

async fn bulk_load<T: BenchStore + Send + Sync>(db: &Arc<T>, rng: &mut Rng) -> Duration {
    let start = Instant::now();
    let mut txn = db.transaction(true);
    for _ in 0..ELEMENTS {
        let (key, value) = gen_pair(rng);
        txn.insert(&key, &value).unwrap();
    }
    txn.commit().await.unwrap();
    Instant::now() - start
}

async fn individual_writes<T: BenchStore + Send + Sync>(
    db: &Arc<T>,
    rng: &mut Rng,
    writes: usize,
) -> Duration {
    let start = Instant::now();
    for _ in 0..writes {
        let mut txn = db.transaction(true);
        let (key, value) = gen_pair(rng);
        txn.insert(&key, &value).unwrap();
        txn.commit().await.unwrap();
    }
    Instant::now() - start
}

async fn batch_writes<T: BenchStore + Send + Sync>(
    db: &Arc<T>,
    rng: &mut Rng,
    writes: usize,
    batch_size: usize,
) -> Duration {
    let start = Instant::now();
    for _ in 0..writes {
        let mut txn = db.transaction(true);
        for _ in 0..batch_size {
            let (key, value) = gen_pair(rng);
            txn.insert(&key, &value).unwrap();
        }
        txn.commit().await.unwrap();
    }
    Instant::now() - start
}

fn random_reads<T: BenchStore + Send + Sync>(db: &Arc<T>) -> Duration {
    let mut rng = make_rng();
    let start = Instant::now();
    let txn = db.transaction(false);
    let mut checksum = 0u64;
    let mut expected_checksum = 0u64;
    for _ in 0..ELEMENTS {
        let (key, value) = gen_pair(&mut rng);
        let result = txn.get(&key).unwrap();
        checksum += result.as_ref()[0] as u64;
        expected_checksum += value[0] as u64;
    }
    assert_eq!(checksum, expected_checksum);
    Instant::now() - start
}

async fn deletes<T: BenchStore + Send + Sync>(
    db: &Arc<T>,
    rng: &mut Rng,
    deletes: usize,
) -> Duration {
    let start = Instant::now();
    let mut txn = db.transaction(true);
    for _ in 0..deletes {
        let (key, _value) = gen_pair(rng);
        txn.delete(&key).unwrap();
    }
    txn.commit().await.unwrap();
    Instant::now() - start
}

fn random_reads_multithreaded<T: BenchStore + Send + Sync>(
    db: &Arc<T>,
    num_threads: usize,
) -> Duration {
    let mut rngs = make_rng_shards(num_threads, ELEMENTS);
    let start = Instant::now();

    thread::scope(|s| {
        for _ in 0..num_threads {
            let db2 = db.clone();
            let mut rng = rngs.pop().unwrap();
            s.spawn(move || {
                let txn = db2.transaction(false);
                let mut checksum = 0u64;
                let mut expected_checksum = 0u64;
                for _ in 0..(ELEMENTS / num_threads) {
                    let (key, value) = gen_pair(&mut rng);
                    let result = txn.get(&key).unwrap();
                    checksum += result.as_ref()[0] as u64;
                    expected_checksum += value[0] as u64;
                }
                assert_eq!(checksum, expected_checksum);
            });
        }
    });

    Instant::now() - start
}

async fn benchmark<T: BenchStore + Send + Sync>(db: T) -> Vec<(String, Duration)> {
    let mut rng = make_rng();
    let db = Arc::new(db);

    let mut results = Vec::new();

    let duration = bulk_load(&db, &mut rng).await;
    results.push(("bulk load".to_string(), duration));

    let duration = individual_writes(&db, &mut rng, 100).await;
    results.push(("individual writes".to_string(), duration));

    let duration = batch_writes(&db, &mut rng, 100, 1000).await;
    results.push(("batch writes".to_string(), duration));

    let duration = random_reads(&db);
    results.push(("random reads".to_string(), duration));

    let duration = deletes(&db, &mut rng, ELEMENTS / 2).await;
    results.push(("deletes".to_string(), duration));

    for num_threads in [4, 8, 16, 32] {
        let duration = random_reads_multithreaded(&db, num_threads);
        results.push((format!("random reads ({} threads)", num_threads), duration));
    }

    results
}

async fn benchmark_redb() -> Vec<(String, Duration)> {
    let tmpdir = create_temp_directory();
    let file_path = tmpdir.path().join("my_file.txt");
    let _ = File::create(file_path.clone()).unwrap();
    let db = redb::Database::builder()
        .set_cache_size(4 * 1024 * 1024 * 1024)
        .create(file_path)
        .unwrap();
    let table = RedbBenchStore::new(&db);
    benchmark(table).await
}

async fn benchmark_rocksdb() -> Vec<(String, Duration)> {
    let tmpdir: TempDir = create_temp_directory();
    let db = rocksdb::TransactionDB::open_default(tmpdir.path()).unwrap();
    let table = RocksdbBenchStore::new(&db);
    benchmark(table).await
}

async fn benchmark_sled() -> Vec<(String, Duration)> {
    let tmpdir: TempDir = create_temp_directory();
    let db = sled::Config::new().path(tmpdir.path()).open().unwrap();
    let table = SledBenchStore::new(&db, tmpdir.path());
    benchmark(table).await
}

async fn benchmark_surrealkv() -> Vec<(String, Duration)> {
    let tmpdir: TempDir = create_temp_directory();
    let mut opts = surrealkv::Options::new();
    opts.dir = tmpdir.path().to_path_buf();
    opts.max_value_threshold = VALUE_SIZE;
    let db = surrealkv::Store::new(opts).unwrap();
    let table = SurrealKVBenchStore::new(&db);
    benchmark(table).await
}

#[tokio::main]
async fn main() {
    let redb_results = benchmark_redb().await;
    let rocksdb_results = benchmark_rocksdb().await;
    let sled_results = benchmark_sled().await;
    let surrealkv_results = benchmark_surrealkv().await;

    let mut rows = vec![Vec::new(); redb_results.len()];

    for results in [
        &redb_results,
        &rocksdb_results,
        &sled_results,
        &surrealkv_results,
    ] {
        for (i, (benchmark, duration)) in results.iter().enumerate() {
            if rows[i].is_empty() {
                rows[i].push(benchmark.to_string());
            }
            rows[i].push(format!("{}ms", duration.as_millis()));
        }
    }

    let mut table = comfy_table::Table::new();
    table.set_width(100);
    table.set_header(["", "redb", "rocksdb", "sled", "surrealkv"]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");
}
