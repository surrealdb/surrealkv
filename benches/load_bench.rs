use bytes::Bytes;
use rand::{thread_rng, Rng};
use std::time::Instant;
use surrealkv::{Options, Store};
use tempdir::TempDir;

#[cfg_attr(any(target_os = "linux", target_os = "macos"), global_allocator)]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn create_temp_directory() -> TempDir {
    TempDir::new("test").unwrap()
}

async fn benchmark_load_times_kv_size() {
    println!("\n=== Benchmarking Load Times - KV Size Combinations with Key Distribution ===");
    println!("Key Size | Value Size | Distribution | Load Time (s) | Store Size (MB)");
    println!("------------------------------------------------------------");

    let num_keys = 1_000_000;
    let key_sizes = vec![32, 64, 128, 256];
    let value_sizes = vec![64, 256, 1024, 4096, 16384, 65536];

    for key_size in key_sizes {
        for value_size in &value_sizes {
            // Test Sequential Keys
            {
                let mut opts = Options::new();
                let temp_dir = create_temp_directory();
                opts.dir = temp_dir.path().to_path_buf();
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
                let store_size = calculate_store_size(&temp_dir);

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
            }

            // Test Random Keys
            {
                let mut opts = Options::new();
                let temp_dir = create_temp_directory();
                opts.dir = temp_dir.path().to_path_buf();
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
                let store_size = calculate_store_size(&temp_dir);

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
                    store_size as f64 / (1024.0 * 1024.0  * 1024.0) // Convert to GB
                );
            }
        }
    }
}

async fn benchmark_load_times_versions() {
    println!("\n=== Benchmarking Load Times - Version Count ===");
    println!("Versions | Keys    | Load Time (s) | Store Size (MB)");
    println!("------------------------------------------------");

    let total_records = 1_000_000;
    let key_size = 256;
    let value_size = 1024;
    let versions = vec![10, 100, 1000];

    for version_count in versions {
        let num_keys = total_records / version_count;

        let mut opts = Options::new();
        let temp_dir = create_temp_directory();
        opts.dir = temp_dir.path().to_path_buf();
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
        let store_size = calculate_store_size(&temp_dir);

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
    }
}

// Helper function to calculate the size of the store directory
fn calculate_store_size(temp_dir: &TempDir) -> u64 {
    let mut total_size = 0;
    for entry in walkdir::WalkDir::new(temp_dir.path()).into_iter().flatten() {
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
