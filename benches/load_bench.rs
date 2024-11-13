use bytes::Bytes;
use rand::{thread_rng, Rng};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use surrealkv::{Options, Store};

#[cfg_attr(any(target_os = "linux", target_os = "macos"), global_allocator)]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn create_local_directory(test_name: &str) -> PathBuf {
    let local_dir = PathBuf::from(format!("./test_data/{}", test_name));
    fs::create_dir_all(&local_dir).unwrap();
    local_dir
}

fn remove_local_directory(local_dir: &PathBuf) {
    for _ in 0..5 {
        if fs::remove_dir_all(local_dir).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

async fn benchmark_load_times_kv_size() {
    println!("\n=== Benchmarking Load Times - KV Size Combinations with Key Distribution ===");
    println!("Key Size | Value Size | Distribution | Load Time (s) | Store Size (GB)");
    println!("------------------------------------------------------------");

    let num_keys = 1_000_000;
    let key_sizes = vec![32, 64, 128, 256];
    let value_sizes = vec![64, 256, 1024, 4096, 16384, 65536];

    for key_size in key_sizes {
        for value_size in &value_sizes {
            // Test Sequential Keys
            {
                let test_name = format!("sequential_keys_{}_{}", key_size, value_size);
                let local_dir = create_local_directory(&test_name);

                let mut opts = Options::new();
                opts.dir = local_dir.clone();
                let store = Store::new(opts.clone()).expect("should create store");

                let default_value = Bytes::from(vec![0x42; *value_size]);
                let keys: Vec<Bytes> = (0..num_keys)
                    .map(|i| Bytes::from(format!("{:0width$}", i, width = key_size)))
                    .collect();

                for key in &keys {
                    let mut txn = store.begin().unwrap();
                    txn.set(key, &default_value).unwrap();
                    txn.commit().await.unwrap();
                }

                store.close().await.unwrap();

                // Calculate store size
                let store_size = calculate_store_size(&local_dir);

                // Measure load time
                let start = Instant::now();
                let _ = Store::new(opts).expect("should create store");
                let duration = start.elapsed();

                println!(
                    "{:8} | {:10} | {:11} | {:12.2} | {:13.2}",
                    key_size,
                    value_size,
                    "Sequential",
                    duration.as_secs_f64(),
                    store_size as f64 / (1024.0 * 1024.0 * 1024.0) // Convert to GB
                );
                store.close().await.unwrap();

                // Remove the local directory
                remove_local_directory(&local_dir);
            }

            // Test Random Keys
            {
                let test_name = format!("random_keys_{}_{}", key_size, value_size);
                let local_dir = create_local_directory(&test_name);

                let mut opts = Options::new();
                opts.dir = local_dir.clone();
                let store = Store::new(opts.clone()).expect("should create store");

                let default_value = Bytes::from(vec![0x42; *value_size]);
                let mut rng = thread_rng();
                let keys: Vec<Bytes> = (0..num_keys)
                    .map(|_| {
                        let key: Vec<u8> = (0..key_size).map(|_| rng.gen()).collect();
                        Bytes::from(key)
                    })
                    .collect();

                for key in &keys {
                    let mut txn = store.begin().unwrap();
                    txn.set(key, &default_value).unwrap();
                    txn.commit().await.unwrap();
                }

                store.close().await.unwrap();

                // Calculate store size
                let store_size = calculate_store_size(&local_dir);

                // Measure load time
                let start = Instant::now();
                let _ = Store::new(opts).expect("should create store");
                let duration = start.elapsed();

                println!(
                    "{:8} | {:10} | {:11} | {:12.2} | {:13.2}",
                    key_size,
                    value_size,
                    "Random",
                    duration.as_secs_f64(),
                    store_size as f64 / (1024.0 * 1024.0 * 1024.0) // Convert to GB
                );
                store.close().await.unwrap();

                // Remove the local directory
                remove_local_directory(&local_dir);
            }
        }
    }
}

async fn benchmark_load_times_versions() {
    println!("\n=== Benchmarking Load Times - Version Count ===");
    println!("Versions | Keys    | Load Time (s) | Store Size (GB)");
    println!("------------------------------------------------");

    let total_records = 1_000_000;
    let key_size = 256;
    let value_size = 1024;
    let versions = vec![10, 100, 1000];

    for version_count in versions {
        let num_keys = total_records / version_count;

        let test_name = format!("versions_{}", version_count);
        let local_dir = create_local_directory(&test_name);

        let mut opts = Options::new();
        opts.dir = local_dir.clone();
        let store = Store::new(opts.clone()).expect("should create store");

        let default_value = Bytes::from(vec![0x42; value_size]);
        let mut rng = thread_rng();
        let keys: Vec<Bytes> = (0..num_keys)
            .map(|_| {
                let key: Vec<u8> = (0..key_size).map(|_| rng.gen()).collect();
                Bytes::from(key)
            })
            .collect();

        for key in &keys {
            for _ in 0..version_count {
                let mut txn = store.begin().unwrap();
                txn.set(key, &default_value).unwrap();
                txn.commit().await.unwrap();
            }
        }

        store.close().await.unwrap();

        // Calculate store size
        let store_size = calculate_store_size(&local_dir);

        let start = Instant::now();
        let _ = Store::new(opts).expect("should create store");
        let duration = start.elapsed();

        println!(
            "{:8} | {:7} | {:12.2} | {:13.2}",
            version_count,
            num_keys,
            duration.as_secs_f64(),
            store_size as f64 / (1024.0 * 1024.0 * 1024.0) // Convert to GB
        );

        store.close().await.unwrap();

        // Remove the local directory
        remove_local_directory(&local_dir);
    }
}

// Helper function to calculate the size of the store directory
fn calculate_store_size(local_dir: &PathBuf) -> u64 {
    let mut total_size = 0;
    for entry in walkdir::WalkDir::new(local_dir).into_iter().flatten() {
        if let Ok(metadata) = entry.metadata() {
            if metadata.is_file() {
                total_size += metadata.len();
            }
        }
    }
    total_size
}

#[tokio::main]
async fn main() {
    println!("Starting Load Time Benchmarks...\n");

    benchmark_load_times_kv_size().await;
    benchmark_load_times_versions().await;

    println!("\nBenchmarks complete!");
}
