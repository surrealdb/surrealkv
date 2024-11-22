use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Bytes;
use bytes::BytesMut;
use humansize::{format_size, BINARY};
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::json;

use surrealkv::{
    restore_repair_files,
    storage::kv::error::Error,
    storage::log::Error as LogError,
    storage::log::SegmentRef,
    storage::log::{Aol, MultiSegmentReader, Options as LogOptions},
    Reader, Record, RecordReader,
};

pub struct DbStats {
    pub total_keys: usize,
    pub total_versions: usize,
    pub max_key_size: usize,
    pub max_value_size: usize,
    pub avg_key_size: f64,
    pub avg_value_size: f64,
    pub total_size: u64,
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

    // Common function to write records and manifest
    fn write_records_and_manifest(
        &self,
        latest_records: BTreeMap<Bytes, Record>,
        destination: &PathBuf,
        pb: &ProgressBar,
    ) -> Result<()> {
        // Create destination directory if it doesn't exist
        std::fs::create_dir_all(destination)?;

        // Copy manifest folder
        let source_manifest = self.db_path.join("manifest");
        let destination_manifest = destination.join("manifest");

        pb.set_message("Copying manifest folder...");
        if source_manifest.exists() {
            // Remove destination manifest if it exists
            if destination_manifest.exists() {
                fs::remove_dir_all(&destination_manifest)?;
            }
            // Copy the manifest directory recursively
            fs::create_dir_all(&destination_manifest)?;
            copy_dir_all(&source_manifest, &destination_manifest)?;
        }

        pb.set_message("Writing latest versions to new location...");

        // Initialize new commit log
        let destination_clog_subdir = destination.join("clog");

        // Setup log options
        let copts = LogOptions::default()
            .with_max_file_size(512 * 1024 * 1024) // 512MB default
            .with_file_extension("clog".to_string());

        // Open the commit log
        let mut destination_commit_log =
            Aol::open(&destination_clog_subdir, &copts).map_err(Error::from)?;

        // Write latest versions to new location
        let mut total_written = 0;
        for record in latest_records.values() {
            let mut buffer = BytesMut::new();
            record.encode(&mut buffer)?;
            destination_commit_log.append(&buffer)?;
            total_written += 1;

            if total_written % 1000 == 0 {
                pb.set_message(format!(
                    "Written {}/{} records",
                    total_written,
                    latest_records.len()
                ));
            }
        }

        // Ensure all writes are persisted
        destination_commit_log.sync()?;

        pb.finish_with_message(format!(
            "Successfully migrated {} records and manifest to {}",
            total_written,
            destination.display()
        ));

        Ok(())
    }

    // Helper to handle corruption info
    fn handle_corruption(&self, corruption_info: Option<(u64, u64)>) -> Result<()> {
        if let Some((corrupted_segment_id, corrupted_offset)) = corruption_info {
            eprintln!(
                "Found corruption in segment {} at offset {}",
                corrupted_segment_id, corrupted_offset
            );
            return Err(anyhow::anyhow!(
                "Corruption found in segment {} at offset {}",
                corrupted_segment_id,
                corrupted_offset
            ));
        }
        Ok(())
    }

    pub fn prune_specific_keys(
        &self,
        keys_file: Option<&PathBuf>,
        keys: Option<&Vec<String>>,
        destination: &PathBuf,
    ) -> Result<()> {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {wide_msg}")
                .unwrap(),
        );

        // Get the target keys
        let target_keys: Vec<String> = if let Some(file) = keys_file {
            std::fs::read_to_string(file)
                .context("Failed to read keys file")?
                .lines()
                .map(String::from)
                .collect()
        } else if let Some(key_list) = keys {
            key_list.clone()
        } else {
            return Err(anyhow::anyhow!("Either keys_file or keys must be provided"));
        };

        pb.set_message(format!(
            "Analyzing records for {} specific keys...",
            target_keys.len()
        ));

        // Convert target keys to Bytes for efficient comparison
        let target_keys_set: BTreeSet<Bytes> = target_keys
            .iter()
            .map(|k| Bytes::copy_from_slice(k.as_bytes()))
            .collect();

        // Map to store all versions of each target key
        let mut key_versions: BTreeMap<Bytes, Vec<Record>> = BTreeMap::new();

        // Read all records and collect all versions for target keys
        let corruption_info = self.read_records(|tx| {
            let record_key = tx.key.clone();

            if target_keys_set.contains(&record_key) {
                key_versions.entry(record_key).or_default().push(tx.clone());
            }
            Ok(())
        })?;

        self.handle_corruption(corruption_info)?;

        // Process versions to keep only the latest one based on tx.id
        let latest_records: BTreeMap<Bytes, Record> = key_versions
            .into_iter()
            .map(|(key, mut versions)| {
                // Sort by id (which is sequential) in descending order
                versions.sort_by(|a, b| b.id.cmp(&a.id));
                // Take the first one (highest id)
                (key, versions.into_iter().next().unwrap())
            })
            .collect();

        let found_keys = latest_records.len();
        pb.set_message(format!(
            "Found {} out of {} requested keys",
            found_keys,
            target_keys.len()
        ));

        // Print keys that were not found
        let found_keys_set: BTreeSet<_> = latest_records.keys().collect();
        let missing_keys: Vec<_> = target_keys_set
            .iter()
            .filter(|k| !found_keys_set.contains(k))
            .collect();

        if !missing_keys.is_empty() {
            println!("\nWarning: The following keys were not found:");
            for key in missing_keys {
                println!("  - {}", String::from_utf8_lossy(key));
            }
        }

        self.write_records_and_manifest(latest_records, destination, &pb)
    }

    pub fn prune_all_keys(&self, destination: &PathBuf) -> Result<()> {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {wide_msg}")
                .unwrap(),
        );
        pb.set_message("Analyzing records...");

        // Map to store all versions of each key
        let mut key_versions: BTreeMap<Bytes, Vec<Record>> = BTreeMap::new();

        // Read all records and collect all versions
        let corruption_info = self.read_records(|tx| {
            let record_key = tx.key.clone();
            key_versions.entry(record_key).or_default().push(tx.clone());
            Ok(())
        })?;

        self.handle_corruption(corruption_info)?;

        // Process versions to keep only the latest one based on tx.id
        let latest_records: BTreeMap<Bytes, Record> = key_versions
            .into_iter()
            .map(|(key, mut versions)| {
                // Sort by id (which is sequential) in descending order
                versions.sort_by(|a, b| b.id.cmp(&a.id));
                // Take the first one (highest id)
                (key, versions.into_iter().next().unwrap())
            })
            .collect();

        pb.set_message(format!("Found {} unique keys", latest_records.len()));

        self.write_records_and_manifest(latest_records, destination, &pb)
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

        // Get file sizes
        if let Ok(metadata) = std::fs::metadata(&self.db_path) {
            stats.total_size = metadata.len();
        }

        pb.finish_with_message("Statistics collection complete!");
        Ok(stats)
    }

    pub fn repair(&self) -> Result<()> {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {wide_msg}")
                .unwrap(),
        );
        pb.set_message("Analyzing commit log for corruption...");

        // Get the commit log directory
        let clog_subdir = self.db_path.join("clog");

        pb.set_message("Repairing commit log...");

        restore_repair_files(clog_subdir.as_path().to_str().unwrap())?;

        pb.finish_with_message(format!(
            "Successfully repaired commit log at {}",
            clog_subdir.display()
        ));

        Ok(())
    }
}

// Helper function to recursively copy directories
fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<()> {
    fs::create_dir_all(&dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dst_path = dst.as_ref().join(entry.file_name());

        if ty.is_dir() {
            copy_dir_all(entry.path(), dst_path)?;
        } else {
            fs::copy(entry.path(), dst_path)?;
        }
    }
    Ok(())
}
