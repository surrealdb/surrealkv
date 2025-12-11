/// Debug test for range query performance investigation
/// 
/// This module contains tests to trace through range query behavior
/// using the crud-bench folder as a real-world dataset.

#[cfg(test)]
mod tests {
    use crate::TreeBuilder;
    use std::path::PathBuf;
    use std::time::Instant;
    use test_log::test;

    /// Opens the crud-bench folder and performs range query tracing
    #[test(tokio::test)]
    async fn test_crud_bench_range_query_performance() {
        // Path to the crud-bench folder
        let crud_bench_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("crud-bench");
        
        if !crud_bench_path.exists() {
            eprintln!("crud-bench folder not found at {:?}, skipping test", crud_bench_path);
            return;
        }

        eprintln!("\n=== Opening crud-bench store at {:?} ===", crud_bench_path);
        
        // Open the store
        let tree = match TreeBuilder::new()
            .with_path(crud_bench_path.clone())
            .with_block_cache_capacity(256 * 1024 * 1024) // 100MB cache
            .with_enable_vlog(true)
            .build() {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("Failed to open store: {:?}", e);
                    return;
                }
            };

        // Print store info
        {
            let manifest = tree.core.level_manifest.read().unwrap();
            let levels = manifest.levels.get_levels();
            eprintln!("\n=== Store Structure ===");
            for (level_idx, level) in levels.iter().enumerate() {
                eprintln!("Level {}: {} tables", level_idx, level.tables.len());
                for table in level.tables.iter().take(5) {
                    if let Some(ref kr) = table.meta.properties.key_range {
                        eprintln!("  Table {}: key_range=[{:?}..{:?}], entries={}", 
                            table.id,
                            String::from_utf8_lossy(&kr.low[..kr.low.len().min(20)]),
                            String::from_utf8_lossy(&kr.high[..kr.high.len().min(20)]),
                            table.meta.properties.num_entries
                        );
                    }
                }
                if level.tables.len() > 5 {
                    eprintln!("  ... and {} more tables", level.tables.len() - 5);
                }
            }
        }

        // Begin a read transaction
        let tx = tree.begin().unwrap();

        eprintln!("\n=== Test 1: Full range scan ===");
        {
            let start = Instant::now();
            let iter = tx.range(&[0u8], &[0xffu8]).unwrap();
            let create_time = start.elapsed();
            
            let iter_start = Instant::now();
            let mut count = 0;
            let mut first_time = None;
            for item in iter {
                let _ = item; // Consume item
                count += 1;
                if count == 1 {
                    first_time = Some(iter_start.elapsed());
                }
            }
            let elapsed = iter_start.elapsed();
            eprintln!("Full range scan: {} items in {:?} (create={:?}, first={:?}, per_item={:?})", 
                count, elapsed, create_time, first_time.unwrap_or_default(), elapsed / count as u32);
        }

        eprintln!("\n=== Test 2: Range query for non-existent keys (before all data) ===");
        {
            // Query for keys that start with 0x00 (likely before all data)
            let start = Instant::now();
            let count = tx.range(&[0x00, 0x00, 0x00], &[0x00, 0x00, 0x01])
                .unwrap()
                .count();
            let elapsed = start.elapsed();
            eprintln!("Non-existent range (before): {} items in {:?}", count, elapsed);
        }

        eprintln!("\n=== Test 3: Range query for non-existent keys (after all data) ===");
        {
            // Query for keys that start with 0xff (likely after all data)
            let start = Instant::now();
            let count = tx.range(&[0xff, 0xff, 0x00], &[0xff, 0xff, 0xff])
                .unwrap()
                .count();
            let elapsed = start.elapsed();
            eprintln!("Non-existent range (after): {} items in {:?}", count, elapsed);
        }

        eprintln!("\n=== Test 4: Range query for middle range (common prefix) ===");
        {
            // Try to find a middle range based on common key patterns
            // This tests seeking to a specific location
            let start = Instant::now();
            let count = tx.range(b"test_key_50000", b"test_key_50100")
                .unwrap()
                .count();
            let elapsed = start.elapsed();
            eprintln!("Middle range (test_key prefix): {} items in {:?}", count, elapsed);
        }

        eprintln!("\n=== Test 4b: Range query starting with '/' (in data range) ===");
        {
            // Query for keys that start with '/' - likely in the data range
            let start_key: &[u8] = b"/!";
            let end_key: &[u8] = b"/!z";
            let start = Instant::now();
            let iter = tx.range(start_key, end_key).unwrap();
            let create_time = start.elapsed();
            
            let iter_start = Instant::now();
            let mut count = 0;
            let mut first_time = None;
            for item in iter {
                let _ = item;
                count += 1;
                if count == 1 {
                    first_time = Some(iter_start.elapsed());
                }
                if count >= 1000 {
                    break; // Limit to 1000 for timing
                }
            }
            let elapsed = iter_start.elapsed();
            eprintln!("Middle data range '/!': {} items (limited to 1000), create={:?}, first={:?}, total={:?}, per_item={:?}",
                count, create_time, first_time.unwrap_or_default(), elapsed, elapsed / count.max(1) as u32);
        }

        eprintln!("\n=== Test 4c: Seek to middle of data and iterate small range ===");
        {
            // Test seeking to middle of data  
            let start_key: &[u8] = b"/!ic\0\0\0\0\0\0\0\x05";
            let end_key: &[u8] = b"/!ic\0\0\0\0\0\0\0\x10";
            let start = Instant::now();
            let iter = tx.range(start_key, end_key).unwrap();
            let create_time = start.elapsed();
            
            let iter_start = Instant::now();
            let count = iter.count();
            let elapsed = iter_start.elapsed();
            eprintln!("Seek to middle '/!ic...': {} items, create={:?}, iterate={:?}",
                count, create_time, elapsed);
        }

        eprintln!("\n=== Test 5: Multiple small range queries ===");
        {
            let start = Instant::now();
            let mut total_count = 0;
            for i in 0..100 {
                let start_key = format!("key_{:06}", i * 1000);
                let end_key = format!("key_{:06}", i * 1000 + 10);
                let count = tx.range(start_key.as_bytes(), end_key.as_bytes())
                    .unwrap()
                    .count();
                total_count += count;
            }
            let elapsed = start.elapsed();
            eprintln!("100 small ranges: {} total items in {:?} ({:?}/query)", 
                total_count, elapsed, elapsed / 100);
        }

        // Test specific key patterns that might be in the crud-bench data
        eprintln!("\n=== Test 6: Prefix-based range queries ===");
        {
            let prefixes = vec![
                ("u:", "v:"),  // Common for user keys
                ("p:", "q:"),  // Common for post keys  
                ("a", "b"),    // Generic
                ("z", "zz"),   // End of alphabet
            ];
            
            for (start, end) in prefixes {
                let query_start = Instant::now();
                let count = tx.range(start.as_bytes(), end.as_bytes())
                    .unwrap()
                    .count();
                let elapsed = query_start.elapsed();
                eprintln!("Range [{:?}..{:?}]: {} items in {:?}", start, end, count, elapsed);
            }
        }

        // Test backward iteration
        eprintln!("\n=== Test 7: Backward iteration ===");
        {
            let start = Instant::now();
            let count = tx.range(&[0u8], &[0xffu8])
                .unwrap()
                .rev()
                .take(1000)
                .count();
            let elapsed = start.elapsed();
            eprintln!("Backward iteration (1000 items): {} items in {:?}", count, elapsed);
        }

        eprintln!("\n=== Test complete ===");
    }

    /// More detailed test focusing on where time is spent
    #[test(tokio::test)]
    async fn test_crud_bench_detailed_timing() {
        let crud_bench_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("crud-bench");
        
        if !crud_bench_path.exists() {
            eprintln!("crud-bench folder not found, skipping test");
            return;
        }

        let tree = match TreeBuilder::new()
            .with_path(crud_bench_path)
            .with_block_cache_capacity(256 * 1024 * 1024) // 100MB cache
            .with_enable_vlog(true)
            .build() {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("Failed to open store: {:?}", e);
                    return;
                }
            };

        let tx = tree.begin().unwrap();

        // Time the iterator creation vs iteration
        eprintln!("\n=== Timing breakdown ===");
        
        for _ in 0..3 {
            // Time 1: Iterator creation
            let create_start = Instant::now();
            let iter = tx.range("nonexistent_prefix_".as_bytes(), "nonexistent_prefix_z".as_bytes()).unwrap();
            let create_time = create_start.elapsed();
            
            // Time 2: First item (includes seek)
            let first_start = Instant::now();
            let iter = iter.peekable();
            let first_time = first_start.elapsed();
            
            // Time 3: Iteration
            let iter_start = Instant::now();
            let count = iter.count();
            let iter_time = iter_start.elapsed();
            
            eprintln!("Create: {:?}, First: {:?}, Iterate({} items): {:?}", 
                create_time, first_time, count, iter_time);
        }

        // Compare with a range that might have data
        eprintln!("\n=== Comparing with potential data range ===");
        
        for _ in 0..3 {
            let create_start = Instant::now();
            let mut iter = tx.range(&[0u8], &[0x10u8]).unwrap();
            let create_time = create_start.elapsed();
            
            let first_start = Instant::now();
            let first_item = iter.next();
            let first_time = first_start.elapsed();
            
            let iter_start = Instant::now();
            let remaining_count = iter.count();
            let iter_time = iter_start.elapsed();
            
            eprintln!("Create: {:?}, First: {:?} (has_data={}), Iterate({} more): {:?}", 
                create_time, first_time, first_item.is_some(), remaining_count, iter_time);
        }
    }
}
