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

use std::env::current_dir;
use std::mem::size_of;
use std::sync::Arc;
use std::{fs, process, thread};
use tempdir::TempDir;

mod common;
use common::*;

use std::fs::File;
use std::time::{Duration, Instant};

const ITERATIONS: usize = 2;
const ELEMENTS: usize = 1_000_000;
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

async fn benchmark<T: BenchDatabase + Send + Sync>(db: T) -> Vec<(String, Duration)> {
    let mut rng = make_rng();
    let mut results = Vec::new();
    let db = Arc::new(db);

    let start = Instant::now();
    let mut txn = db.write_transaction();
    {
        for _ in 0..ELEMENTS {
            let (key, value) = gen_pair(&mut rng);
            txn.insert(&key, &value).unwrap();
        }
    }
    txn.commit().await.unwrap();

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Bulk loaded {} items in {}ms",
        T::store_name(),
        ELEMENTS,
        duration.as_millis()
    );
    results.push(("bulk load".to_string(), duration));

    let start = Instant::now();
    let writes = 100;
    {
        for _ in 0..writes {
            let mut txn = db.write_transaction();
            let (key, value) = gen_pair(&mut rng);
            txn.insert(&key, &value).unwrap();
            txn.commit().await.unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} individual items in {}ms",
        T::store_name(),
        writes,
        duration.as_millis()
    );
    results.push(("individual writes".to_string(), duration));

    let start = Instant::now();
    let batch_size = 1000;
    {
        for _ in 0..writes {
            let mut txn = db.write_transaction();
            for _ in 0..batch_size {
                let (key, value) = gen_pair(&mut rng);
                txn.insert(&key, &value).unwrap();
            }
            txn.commit().await.unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} x {} items in {}ms",
        T::store_name(),
        writes,
        batch_size,
        duration.as_millis()
    );
    results.push(("batch writes".to_string(), duration));

    let txn = db.read_transaction();
    {
        for _ in 0..ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            for _ in 0..ELEMENTS {
                let (key, value) = gen_pair(&mut rng);
                let result = txn.get(&key).unwrap();
                checksum += result.as_ref()[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random read {} items in {}ms",
                T::store_name(),
                ELEMENTS,
                duration.as_millis()
            );
            results.push(("random reads".to_string(), duration));
        }
    }
    drop(txn);

    for num_threads in [4, 8, 16, 32] {
        let mut rngs = make_rng_shards(num_threads, ELEMENTS);
        let start = Instant::now();

        thread::scope(|s| {
            for _ in 0..num_threads {
                let db2 = db.clone();
                let mut rng = rngs.pop().unwrap();
                s.spawn(move || {
                    let txn = db2.read_transaction();
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

        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Random read ({} threads) {} items in {}ms",
            T::store_name(),
            num_threads,
            ELEMENTS,
            duration.as_millis()
        );
        results.push((format!("random reads ({num_threads} threads)"), duration));
    }

    let start = Instant::now();
    let deletes = ELEMENTS / 2;
    {
        let mut rng = make_rng();
        let mut txn = db.write_transaction();
        for _ in 0..deletes {
            let (key, _value) = gen_pair(&mut rng);
            txn.remove(&key).unwrap();
        }
        txn.commit().await.unwrap();
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Removed {} items in {}ms",
        T::store_name(),
        deletes,
        duration.as_millis()
    );
    results.push(("removals".to_string(), duration));

    results
}

#[tokio::main]
async fn main() {
    let tmpdir = current_dir().unwrap().join(".benchmark");
    fs::create_dir(&tmpdir).unwrap();

    let tmpdir2 = tmpdir.clone();
    ctrlc::set_handler(move || {
        fs::remove_dir_all(&tmpdir2).unwrap();
        process::exit(1);
    })
    .unwrap();

    let redb_latency_results = {
        let tmpdir = create_temp_directory();
        let file_path = tmpdir.path().join("my_file.txt");
        let _ = File::create(file_path.clone()).unwrap();
        let db = redb::Database::builder()
            .set_cache_size(4 * 1024 * 1024 * 1024)
            .create(file_path)
            .unwrap();
        let table = RedbBenchDatabase::new(&db);
        benchmark(table).await
    };

    let rocksdb_results = {
        let tmpdir: TempDir = create_temp_directory();
        let db = rocksdb::TransactionDB::open_default(tmpdir.path()).unwrap();
        let table = RocksdbBenchDatabase::new(&db);
        benchmark(table).await
    };

    let sled_results = {
        let tmpdir: TempDir = create_temp_directory();
        let db = sled::Config::new().path(tmpdir.path()).open().unwrap();
        let table = SledBenchDatabase::new(&db, tmpdir.path());
        benchmark(table).await
    };

    let surrealkv_results = {
        let tmpdir: TempDir = create_temp_directory();
        let mut opts = surrealkv::Options::new();
        opts.dir = tmpdir.path().to_path_buf();
        opts.max_value_threshold = 200;
        opts.max_tx_entries = 10000000;
        let db = surrealkv::Store::new(opts).unwrap();
        let table = SurrealKVBenchDatabase::new(&db);
        benchmark(table).await
    };

    fs::remove_dir_all(&tmpdir).unwrap();

    let mut rows = Vec::new();

    for (benchmark, _duration) in &redb_latency_results {
        rows.push(vec![benchmark.to_string()]);
    }

    for results in [
        redb_latency_results,
        rocksdb_results,
        sled_results,
        surrealkv_results,
    ] {
        for (i, (_benchmark, duration)) in results.iter().enumerate() {
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
