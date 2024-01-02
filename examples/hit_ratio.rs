use core::num;

use caches::{AdaptiveCache, Cache, LRUCache, SegmentedCache, TwoQueueCache, WTinyLFUCache};
use hashbrown::HashSet;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use surrealkv::storage::cache::s3fifo::Cache as s3fifo_cache;

fn lru_cache(cases: Vec<(usize, Vec<u64>)>, capacity: usize) -> Vec<(usize, f64)> {
    let mut result: Vec<(usize, f64)> = Vec::with_capacity(cases.len());

    cases.iter().for_each(|total| {
        let mut l = LRUCache::new(capacity).unwrap();

        let mut hit = 0u64;
        let mut miss = 0u64;

        (0..total.0).for_each(|v| {
            let k = total.1[v];
            let _ = l.put(k, k);
        });

        (0..total.0).for_each(|v| {
            let k = total.1[v];
            if l.get(&k).is_some() {
                hit += 1;
            } else {
                miss += 1;
            }
        });

        let hit_ratio = ((hit as f64) / (total.0 as f64)) * 100.0;
        result.push((total.0, hit_ratio));
    });

    result
}

fn wtinylfu_cache(cases: Vec<(usize, Vec<u64>)>, capacity: usize) -> Vec<(usize, f64)> {
    let mut result: Vec<(usize, f64)> = Vec::with_capacity(cases.len());

    cases.iter().for_each(|total| {
        let mut l = WTinyLFUCache::with_sizes(82, 6488, 1622, capacity).unwrap();

        let mut hit = 0u64;
        let mut miss = 0u64;

        (0..total.0).for_each(|v| {
            let k = total.1[v];
            let _ = l.put(k, k);
        });

        (0..total.0).for_each(|v| {
            let k = total.1[v];
            if l.get(&k).is_some() {
                hit += 1;
            } else {
                miss += 1;
            }
        });

        let hit_ratio = ((hit as f64) / (total.0 as f64)) * 100.0;
        result.push((total.0, hit_ratio));
    });

    result
}

fn s3fifo_cache(cases: Vec<(usize, Vec<u64>)>, capacity: usize) -> Vec<(usize, f64)> {
    let mut result: Vec<(usize, f64)> = Vec::with_capacity(cases.len());

    cases.iter().for_each(|total| {
        let mut l = s3fifo_cache::new(capacity);
        let mut hit = 0u64;
        let mut miss = 0u64;

        (0..total.0).for_each(|v| {
            let k = total.1[v];
            let _ = l.insert(k, k);
        });

        (0..total.0).for_each(|v| {
            let k = total.1[v];
            if l.get(&k).is_some() {
                hit += 1;
            } else {
                miss += 1;
            }
        });

        let hit_ratio = ((hit as f64) / (total.0 as f64)) * 100.0;
        result.push((total.0, hit_ratio));
    });

    result
}

fn main() {
    let cases: Vec<usize> = [100_000].to_vec();
    // let capacity = [250usize, 500, 750, 1000, 1250, 1500, 1750, 2000];
    let capacity = [10usize, 25, 50, 100, 200, 400, 800];

    let random_numbers: Vec<(usize, Vec<u64>)> = cases
        .iter()
        .map(|total| {
            let total = *total;
            let mut nums = Vec::with_capacity(total);
            let mut rng = rand::thread_rng();
            for _ in 0..total {
                nums.push(rng.gen::<u64>());
            }
            (total, nums)
        })
        .collect();

    for cap in capacity {
        println!("Capacity: {}", cap);
        println!(
            "LRU Hit Ratio: {:?}",
            lru_cache(random_numbers.clone(), cap)
        );

        // println!(
        //     "WTinyLFUCache Hit Ratio: {:?}",
        //     wtinylfu_cache(random_numbers.clone(), cap)
        // );

        println!(
            "S3fifo Hit Ratio: {:?}",
            s3fifo_cache(random_numbers.clone(), cap)
        );
    }
}
