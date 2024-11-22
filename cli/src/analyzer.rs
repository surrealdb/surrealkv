use anyhow::{Context, Result};
use humansize::{format_size, BINARY};
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use surrealkv::{
    storage::kv::error::Error, storage::log::Error as LogError, storage::log::MultiSegmentReader,
    storage::log::SegmentRef, Reader, Record, RecordReader,
};

pub struct DbStats {
    pub total_keys: usize,
    pub total_versions: usize,
    pub max_key_size: usize,
    pub max_value_size: usize,
    pub avg_key_size: f64,
    pub avg_value_size: f64,
    pub total_size: u64,
    pub version_distribution: HashMap<usize, usize>,
}

impl DbStats {
    pub fn display(&self) {
        println!("Database Statistics:");
        println!("-------------------");
        println!("Total Keys: {}", self.total_keys);
        println!("Total Versions: {}", self.total_versions);
        println!("Max Key Size: {}", format_size(self.max_key_size, BINARY));
        println!(
            "Max Value Size: {}",
            format_size(self.max_value_size, BINARY)
        );
        println!("Average Key Size: {:.2} bytes", self.avg_key_size);
        println!("Average Value Size: {:.2} bytes", self.avg_value_size);
        println!(
            "Total Database Size: {}",
            format_size(self.total_size, BINARY)
        );

        println!("\nVersion Distribution:");
        let mut version_counts: Vec<_> = self.version_distribution.iter().collect();
        version_counts.sort_by_key(|&(k, _)| k);
        for (versions, count) in version_counts {
            println!("{} versions: {} keys", versions, count);
        }
    }
}

pub struct Analyzer {
    db_path: PathBuf,
}

impl Analyzer {
    pub fn new(db_path: PathBuf) -> Result<Self> {
        Ok(Self { db_path })
    }

    // Common function to read through files
    fn read_records<F>(&self, mut callback: F) -> Result<Option<(u64, u64)>>
    where
        F: FnMut(&Record) -> Result<()>,
    {
        let clog_subdir = self.db_path.join("clog");
        let sr = SegmentRef::read_segments_from_directory(clog_subdir.as_path())
            .expect("should read segments");

        let reader = MultiSegmentReader::new(sr)?;
        let reader = Reader::new_from(reader);
        let mut tx_reader = RecordReader::new(reader);
        let mut tx = Record::new();
        let mut corruption_info: Option<(u64, u64)> = None;

        loop {
            tx.reset();

            match tx_reader.read_into(&mut tx) {
                Ok(_) => {
                    callback(&tx)?;
                }
                Err(Error::LogError(LogError::Eof)) => break,
                Err(Error::LogError(LogError::Corruption(err))) => {
                    eprintln!("Corruption error: {:?}", err);
                    corruption_info = Some((err.segment_id, err.offset));
                    break;
                }
                Err(err) => return Err(err.into()),
            };
        }

        Ok(corruption_info)
    }

    pub fn analyze_top_versions(&self, format: &str, top_n: usize) -> Result<()> {
        let start = Instant::now();
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {wide_msg}")
                .unwrap(),
        );
        pb.set_message("Analyzing key versions...");

        let mut version_counts: BTreeMap<Vec<u8>, usize> = BTreeMap::new();

        let corruption_info = self.read_records(|tx| {
            *version_counts.entry(tx.key.to_vec()).or_insert(0) += 1;
            Ok(())
        })?;

        if let Some((corrupted_segment_id, corrupted_offset)) = corruption_info {
            eprintln!(
                "Data corrupted at segment with id: {} and offset: {}",
                corrupted_segment_id, corrupted_offset
            );
        }

        let mut counts: Vec<_> = version_counts.into_iter().collect();
        counts.sort_by(|a, b| b.1.cmp(&a.1));
        let top_keys = counts.into_iter().take(top_n);

        pb.finish_with_message("Analysis complete!");

        match format {
            "json" => {
                let json_output = json!({
                    "top_keys": top_keys.map(|(key, count)| {
                        json!({
                            "key": String::from_utf8_lossy(&key),
                            "versions": count
                        })
                    }).collect::<Vec<_>>()
                });
                println!("{}", serde_json::to_string_pretty(&json_output)?);
            }
            "table" => {
                println!("\nTop 10 Keys by Version Count:");
                println!("----------------------------");
                for (key, count) in top_keys {
                    println!("{}: {} versions", String::from_utf8_lossy(&key), count);
                }
            }
            _ => return Err(anyhow::anyhow!("Unsupported format: {}", format)),
        }

        println!("\nAnalysis completed in {:.2?}", start.elapsed());
        Ok(())
    }

    pub fn prune_specific_keys(
        &self,
        keys_file: Option<&PathBuf>,
        keys: Option<&Vec<String>>,
    ) -> Result<()> {
        let keys: Vec<String> = if let Some(file) = keys_file {
            // Read from file
            std::fs::read_to_string(file)
                .context("Failed to read keys file")?
                .lines()
                .map(String::from)
                .collect()
        } else if let Some(key_list) = keys {
            // Use provided key list
            key_list.clone()
        } else {
            return Err(anyhow::anyhow!("Either keys_file or keys must be provided"));
        };

        let pb = ProgressBar::new(keys.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap(),
        );

        for key in keys {
            // TODO: Implement pruning logic
            pb.inc(1);
        }

        pb.finish_with_message("Pruning complete!");
        Ok(())
    }

    pub fn prune_all_keys(&self) -> Result<()> {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {wide_msg}")
                .unwrap(),
        );

        // TODO: Implement pruning logic using store APIs

        pb.finish_with_message("All keys pruned successfully!");
        Ok(())
    }

    pub fn collect_stats(&self) -> Result<DbStats> {
        let pb = ProgressBar::new_spinner();
        pb.set_message("Collecting database statistics...");

        let mut stats = DbStats {
            total_keys: 0,
            total_versions: 0,
            max_key_size: 0,
            max_value_size: 0,
            avg_key_size: 0.0,
            avg_value_size: 0.0,
            total_size: 0,
            version_distribution: HashMap::new(),
        };

        let mut key_sizes = Vec::new();
        let mut value_sizes = Vec::new();
        let mut unique_keys = BTreeMap::new();

        let corruption_info = self.read_records(|tx| {
            *unique_keys.entry(tx.key.to_vec()).or_insert(0) += 1;

            let key_size = tx.key.len();
            let value_size = tx.value_len as usize;

            key_sizes.push(key_size);
            value_sizes.push(value_size);

            stats.max_key_size = stats.max_key_size.max(key_size);
            stats.max_value_size = stats.max_value_size.max(value_size);

            Ok(())
        })?;

        if let Some((corrupted_segment_id, corrupted_offset)) = corruption_info {
            eprintln!(
                "Data corrupted at segment with id: {} and offset: {}",
                corrupted_segment_id, corrupted_offset
            );
        }

        // Calculate statistics
        stats.total_keys = unique_keys.len();
        stats.total_versions = unique_keys.values().sum();

        // Calculate averages
        if !key_sizes.is_empty() {
            stats.avg_key_size = key_sizes.iter().sum::<usize>() as f64 / key_sizes.len() as f64;
            stats.avg_value_size =
                value_sizes.iter().sum::<usize>() as f64 / value_sizes.len() as f64;
        }

        // Calculate version distribution
        for (_, version_count) in unique_keys {
            *stats.version_distribution.entry(version_count).or_insert(0) += 1;
        }

        // Get file sizes
        if let Ok(metadata) = std::fs::metadata(&self.db_path) {
            stats.total_size = metadata.len();
        }

        pb.finish_with_message("Statistics collection complete!");
        Ok(stats)
    }
}
