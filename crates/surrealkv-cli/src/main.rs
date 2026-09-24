use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use tabled::settings::Style;
use tabled::{Table as DisplayTable, Tabled};

use surrealkv::inspect::{self, DbInfo, ManifestInfo, SstInfo};
use surrealkv::{LSMIterator, TreeBuilder};

#[derive(Parser)]
#[command(
	name = "skv",
	author = "SurrealDB Team",
	version,
	about = "Operator and developer inspection, dump, and diagnostic CLI for SurrealKV"
)]
struct Cli {
	#[command(subcommand)]
	command: Commands,
}

#[derive(Subcommand)]
enum Commands {
	/// Inspect database directory and print overall storage health
	Inspect {
		/// Path to database directory
		path: PathBuf,
	},

	/// Dump and analyze LevelManifest state and SSTable hierarchy
	Manifest {
		/// Path to database directory or MANIFEST file
		path: PathBuf,
	},

	/// Dump and verify an individual SSTable (.sst file)
	Sst {
		/// Path to .sst file
		path: PathBuf,

		/// Dump all key-value entries
		#[arg(long, default_value_t = false)]
		dump_keys: bool,

		/// Recalculate and verify CRC32 for every data block
		#[arg(long, default_value_t = false)]
		verify_checksum: bool,

		/// Display binary keys and values in hexadecimal format
		#[arg(long, default_value_t = false)]
		hex: bool,

		/// Only count entries without displaying details
		#[arg(long, default_value_t = false)]
		count_only: bool,
	},

	/// Dump and decode WAL segments
	Wal {
		/// Path to database directory or wal/ directory
		path: PathBuf,
	},

	/// Perform a point read for a key
	Get {
		/// Path to database directory
		path: PathBuf,

		/// Key to look up
		key: String,

		/// Treat key and value as hexadecimal strings
		#[arg(long, default_value_t = false)]
		hex: bool,
	},

	/// Scan a range or prefix of keys
	Scan {
		/// Path to database directory
		path: PathBuf,

		/// Start key (inclusive)
		#[arg(long)]
		start: Option<String>,

		/// End key (exclusive)
		#[arg(long)]
		end: Option<String>,

		/// Prefix to filter keys
		#[arg(long)]
		prefix: Option<String>,

		/// Maximum items to return
		#[arg(long, default_value_t = 50)]
		limit: usize,

		/// Only display keys (omit values)
		#[arg(long, default_value_t = false)]
		keys_only: bool,

		/// Treat keys and values as hexadecimal strings
		#[arg(long, default_value_t = false)]
		hex: bool,
	},

	/// Insert or update a key-value pair
	Put {
		/// Path to database directory
		path: PathBuf,

		/// Key to insert
		key: String,

		/// Value to insert
		value: String,

		/// Treat key and value as hexadecimal strings
		#[arg(long, default_value_t = false)]
		hex: bool,
	},

	/// Delete a key
	Delete {
		/// Path to database directory
		path: PathBuf,

		/// Key to delete
		key: String,

		/// Treat key as hexadecimal string
		#[arg(long, default_value_t = false)]
		hex: bool,
	},

	/// Verify CRC32 checksums across all SSTable blocks in the database
	Scrub {
		/// Path to database directory
		path: PathBuf,
	},

	/// Explicitly migrate a RocksDB or SurrealKV V1 store to V2
	Migrate {
		/// Source database path (RocksDB or SurrealKV V1)
		source: PathBuf,

		/// Destination SurrealKV V2 database path
		destination: PathBuf,
	},

	/// Run high-concurrency synthetic read/write benchmarks
	Bench {
		/// Path to temporary database directory
		path: PathBuf,

		/// Total number of keys
		#[arg(long, default_value_t = 50_000)]
		keys: usize,

		/// Number of concurrent workers
		#[arg(long, default_value_t = 8)]
		concurrency: usize,
	},
}

#[derive(Tabled)]
struct LevelRow {
	#[tabled(rename = "Level")]
	level: usize,
	#[tabled(rename = "Tables")]
	tables: usize,
	#[tabled(rename = "Size (Bytes)")]
	size_bytes: u64,
	#[tabled(rename = "Size (MB)")]
	size_mb: String,
}

#[derive(Tabled)]
struct TableManifestRow {
	#[tabled(rename = "Level")]
	level: usize,
	#[tabled(rename = "Table ID")]
	id: u64,
	#[tabled(rename = "Size (Bytes)")]
	size: u64,
	#[tabled(rename = "Smallest Key")]
	smallest: String,
	#[tabled(rename = "Largest Key")]
	largest: String,
}

#[derive(Tabled)]
struct WalFileRow {
	#[tabled(rename = "WAL Segment File")]
	file_name: String,
	#[tabled(rename = "Size (Bytes)")]
	size: u64,
	#[tabled(rename = "Size (KB)")]
	size_kb: String,
}

#[tokio::main]
async fn main() -> Result<()> {
	let cli = Cli::parse();

	match cli.command {
		Commands::Inspect {
			path,
		} => {
			cmd_inspect(&path)?;
		}
		Commands::Manifest {
			path,
		} => {
			cmd_manifest(&path)?;
		}
		Commands::Sst {
			path,
			dump_keys,
			verify_checksum,
			hex,
			count_only,
		} => {
			cmd_sst(&path, dump_keys, verify_checksum, hex, count_only)?;
		}
		Commands::Wal {
			path,
		} => {
			cmd_wal(&path)?;
		}
		Commands::Get {
			path,
			key,
			hex,
		} => {
			cmd_get(&path, &key, hex).await?;
		}
		Commands::Scan {
			path,
			start,
			end,
			prefix,
			limit,
			keys_only,
			hex,
		} => {
			cmd_scan(&path, start, end, prefix, limit, keys_only, hex).await?;
		}
		Commands::Put {
			path,
			key,
			value,
			hex,
		} => {
			cmd_put(&path, &key, &value, hex).await?;
		}
		Commands::Delete {
			path,
			key,
			hex,
		} => {
			cmd_delete(&path, &key, hex).await?;
		}
		Commands::Scrub {
			path,
		} => {
			cmd_scrub(&path)?;
		}
		Commands::Migrate {
			source,
			destination,
		} => {
			cmd_migrate(&source, &destination).await?;
		}
		Commands::Bench {
			path,
			keys,
			concurrency,
		} => {
			cmd_bench(&path, keys, concurrency).await?;
		}
	}

	Ok(())
}

fn cmd_inspect(path: &Path) -> Result<()> {
	println!("{}", "=== SurrealKV Database Inspection ===".bold().cyan());
	println!("Path: {}", path.display().to_string().yellow());

	let info: DbInfo = inspect::inspect_db(path)
		.with_context(|| format!("Failed to inspect database at {}", path.display()))?;

	println!("Manifest Version: {}", info.manifest_version);
	println!("Next Table ID:    {}", info.next_table_id);
	println!("Log Number:       {}", info.log_number);
	println!("Last Sequence:    {}", info.last_sequence);
	println!("Total SSTables:   {}", info.total_tables);
	println!("Total SST Size:   {:.2} MB", info.total_sst_bytes as f64 / 1_048_576.0);
	println!("Total WAL Size:   {:.2} KB", info.total_wal_bytes as f64 / 1024.0);
	println!("Total VLog Size:  {:.2} MB", info.total_vlog_bytes as f64 / 1_048_576.0);

	if info.is_locked {
		println!("Lockfile Status:  {} (PID: {:?})", "LOCKED".red().bold(), info.lock_pid);
	} else {
		println!("Lockfile Status:  {} (Last PID: {:?})", "UNLOCKED".green(), info.lock_pid);
	}

	println!("\n{}", "Level Breakdown:".bold());
	let rows: Vec<LevelRow> = info
		.levels
		.into_iter()
		.map(|l| LevelRow {
			level: l.level,
			tables: l.table_count,
			size_bytes: l.total_bytes,
			size_mb: format!("{:.2}", l.total_bytes as f64 / 1_048_576.0),
		})
		.collect();

	let table = DisplayTable::new(rows).with(Style::rounded()).to_string();
	println!("{table}");

	Ok(())
}

fn cmd_manifest(path: &Path) -> Result<()> {
	println!("{}", "=== LevelManifest State ===".bold().cyan());
	let info: ManifestInfo = inspect::dump_manifest(path)
		.with_context(|| format!("Failed to dump manifest at {}", path.display()))?;

	println!("Manifest Format Version: {}", info.version);
	println!("Next Table ID:           {}", info.next_table_id);
	println!("Log Number:              {}", info.log_number);
	println!("Last Sequence Number:    {}", info.last_sequence);

	let mut rows = Vec::new();
	for lvl in info.levels {
		for t in lvl.tables {
			rows.push(TableManifestRow {
				level: lvl.level,
				id: t.id,
				size: t.file_size,
				smallest: format_bytes(&t.smallest_key, false),
				largest: format_bytes(&t.largest_key, false),
			});
		}
	}

	if rows.is_empty() {
		println!("\n{}", "No active SSTables registered in manifest.".yellow());
	} else {
		println!("\n{}", "Active SSTable Hierarchy:".bold());
		let table = DisplayTable::new(rows).with(Style::rounded()).to_string();
		println!("{table}");
	}

	if !info.snapshots.is_empty() {
		println!("\n{}", "Open Snapshots:".bold());
		for s in info.snapshots {
			println!(
				"  - Snapshot ID: {}, Sequence: {}, Timestamp: {}",
				s.id, s.sequence, s.timestamp
			);
		}
	}

	Ok(())
}

fn cmd_sst(
	path: &Path,
	dump_keys: bool,
	verify_checksum: bool,
	hex: bool,
	count_only: bool,
) -> Result<()> {
	println!("{}", "=== SSTable File Inspection ===".bold().cyan());
	println!("File: {}", path.display().to_string().yellow());

	let info: SstInfo = inspect::dump_sst(path, dump_keys || count_only, verify_checksum)
		.with_context(|| format!("Failed to inspect SSTable at {}", path.display()))?;

	println!("File Size:          {} bytes", info.file_size);
	println!("Format Version:     {}", info.format_version);
	println!("Checksum Type:      {}", info.checksum_type);
	println!("Total Entries:      {}", info.total_entries);
	println!("Raw Keys Size:      {} bytes", info.raw_key_size);
	println!("Raw Values Size:    {} bytes", info.raw_value_size);
	println!("Data Blocks:        {}", info.data_blocks_count);
	println!("Index Partitions:   {}", info.index_blocks_count);
	println!("Filter Size:        {} bytes", info.filter_size);

	if let Some(blocks) = info.verified_blocks {
		println!("Checksum Integrity: {} (verified {} blocks)", "PASSED".green().bold(), blocks);
	}

	if count_only {
		println!("Counted entries:    {}", info.keys.as_ref().map_or(0, |k| k.len()));
		return Ok(());
	}

	if dump_keys {
		if let Some(keys) = info.keys {
			println!("\n{}", format!("Keys ({}) :", keys.len()).bold());
			for (i, entry) in keys.iter().enumerate() {
				let k_str = format_bytes(&entry.user_key, hex);
				let v_str = format_bytes(&entry.value_preview, hex);
				println!(
					"  [{:04}] seq={:<6} kind={:<12} key={} val={}",
					i, entry.seq_num, entry.kind, k_str, v_str
				);
			}
		}
	}

	Ok(())
}

fn cmd_wal(path: &Path) -> Result<()> {
	println!("{}", "=== WAL Segments Inspection ===".bold().cyan());
	let info = inspect::dump_wal(path)
		.with_context(|| format!("Failed to inspect WAL at {}", path.display()))?;

	println!(
		"Total WAL Size: {:.2} KB ({} files)",
		info.total_bytes as f64 / 1024.0,
		info.segment_files.len()
	);

	let rows: Vec<WalFileRow> = info
		.segment_files
		.into_iter()
		.map(|s| WalFileRow {
			file_name: s.file_name,
			size: s.file_size,
			size_kb: format!("{:.2}", s.file_size as f64 / 1024.0),
		})
		.collect();

	if rows.is_empty() {
		println!("\n{}", "No WAL segments found.".yellow());
	} else {
		let table = DisplayTable::new(rows).with(Style::rounded()).to_string();
		println!("{table}");
	}

	Ok(())
}

async fn cmd_get(path: &Path, key_str: &str, hex: bool) -> Result<()> {
	let key_bytes = parse_input_bytes(key_str, hex)?;
	let tree = TreeBuilder::new()
		.with_path(path.to_path_buf())
		.build()
		.context("Failed to open database")?;

	let tx = tree.begin()?;
	let val = tx.get(&key_bytes)?;

	match val {
		Some(v) => {
			println!("{}", format_bytes(&v, hex));
		}
		None => {
			eprintln!("{}", "(not found)".yellow());
			std::process::exit(1);
		}
	}

	tree.close().await?;
	Ok(())
}

async fn cmd_scan(
	path: &Path,
	start: Option<String>,
	end: Option<String>,
	prefix: Option<String>,
	limit: usize,
	keys_only: bool,
	hex: bool,
) -> Result<()> {
	let tree = TreeBuilder::new()
		.with_path(path.to_path_buf())
		.build()
		.context("Failed to open database")?;

	let tx = tree.begin()?;

	let (s_bytes, e_bytes) = if let Some(pref) = prefix {
		let p_bytes = parse_input_bytes(&pref, hex)?;
		let mut end_b = p_bytes.clone();
		if let Some(last) = end_b.last_mut() {
			*last = last.saturating_add(1);
		}
		(p_bytes, end_b)
	} else {
		let s = match start {
			Some(ref k) => parse_input_bytes(k, hex)?,
			None => Vec::new(),
		};
		let e = match end {
			Some(ref k) => parse_input_bytes(k, hex)?,
			None => vec![0xff; 32],
		};
		(s, e)
	};

	let mut iter = tx.range(&s_bytes, &e_bytes)?;
	iter.seek_first()?;
	let mut count = 0;

	while iter.valid() && count < limit {
		let k = iter.key().user_key().to_vec();
		let k_str = format_bytes(&k, hex);

		if keys_only {
			println!("{k_str}");
		} else {
			let v = iter.value()?;
			let v_str = format_bytes(&v, hex);
			println!("{k_str} => {v_str}");
		}

		count += 1;
		if !iter.next()? {
			break;
		}
	}

	tree.close().await?;
	Ok(())
}

async fn cmd_put(path: &Path, key_str: &str, val_str: &str, hex: bool) -> Result<()> {
	let key_bytes = parse_input_bytes(key_str, hex)?;
	let val_bytes = parse_input_bytes(val_str, hex)?;

	let tree = TreeBuilder::new()
		.with_path(path.to_path_buf())
		.build()
		.context("Failed to open database")?;

	let mut tx = tree.begin()?;
	tx.set(&key_bytes, &val_bytes)?;
	tx.commit().await?;

	println!("{}", "OK".green().bold());
	tree.close().await?;
	Ok(())
}

async fn cmd_delete(path: &Path, key_str: &str, hex: bool) -> Result<()> {
	let key_bytes = parse_input_bytes(key_str, hex)?;

	let tree = TreeBuilder::new()
		.with_path(path.to_path_buf())
		.build()
		.context("Failed to open database")?;

	let mut tx = tree.begin()?;
	tx.delete(&key_bytes)?;
	tx.commit().await?;

	println!("{}", "OK".green().bold());
	tree.close().await?;
	Ok(())
}

fn cmd_scrub(path: &Path) -> Result<()> {
	println!("{}", "=== SurrealKV Checksum Integrity Scrub ===".bold().cyan());
	println!("Database: {}", path.display().to_string().yellow());

	let report = inspect::scrub_db(path)
		.with_context(|| format!("Failed to scrub database at {}", path.display()))?;

	println!("Total SSTables:    {}", report.total_tables);
	println!("Verified SSTables: {}", report.verified_tables);
	println!("Verified Blocks:   {}", report.total_blocks);

	if report.corrupt_blocks == 0 && report.errors.is_empty() {
		println!(
			"\n{} All {} blocks in {} tables verified without corruption.",
			"SCRUB PASSED:".green().bold(),
			report.total_blocks,
			report.verified_tables
		);
		Ok(())
	} else {
		eprintln!(
			"\n{} Detected {} corrupted blocks!",
			"SCRUB FAILED:".red().bold(),
			report.corrupt_blocks
		);
		for err in report.errors {
			eprintln!("  - {}", err.red());
		}
		std::process::exit(1);
	}
}

async fn cmd_migrate(source: &Path, destination: &Path) -> Result<()> {
	println!("{}", "=== SurrealKV Offline Migration Tool ===".bold().cyan());
	println!("Source:      {}", source.display().to_string().yellow());
	println!("Destination: {}", destination.display().to_string().green());

	let is_rocksdb = surrealkv_compat_rocksdb::is_rocksdb_dir(source);
	let is_v1 = surrealkv_compat_v1::is_v1_dir(source);

	if !is_rocksdb && !is_v1 {
		bail!(
			"Source directory at {} is not a recognized RocksDB or SurrealKV V1 database",
			source.display()
		);
	}

	let format_name = if is_rocksdb {
		"RocksDB BlockBasedTable"
	} else {
		"SurrealKV V1"
	};
	println!("Detected format: {}", format_name.bold().cyan());

	let records = if is_rocksdb {
		surrealkv_compat_rocksdb::read_all_latest(source)?
	} else {
		surrealkv_compat_v1::read_all_latest(source)?
	};

	println!("Read {} live records from source.", records.len());

	let pb = ProgressBar::new(records.len() as u64);
	pb.set_style(
		ProgressStyle::default_bar()
			.template(
				"{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
			)
			.unwrap(),
	);

	let dest_tree = TreeBuilder::new()
		.with_path(destination.to_path_buf())
		.build()
		.context("Failed to initialize destination SurrealKV V2 tree")?;

	let mut tx = dest_tree.begin()?;
	let mut count = 0;

	for (k, v) in records {
		tx.set(&k, &v)?;
		count += 1;
		pb.inc(1);

		if count % 2000 == 0 {
			tx.commit().await?;
			tx = dest_tree.begin()?;
		}
	}

	if count % 2000 != 0 {
		tx.commit().await?;
	}

	pb.finish_with_message("Writing complete");
	dest_tree.flush_wal(true)?;
	dest_tree.close().await?;

	println!("{}", "Migration successfully completed to SurrealKV V2!".green().bold());
	Ok(())
}

async fn cmd_bench(path: &Path, keys: usize, concurrency: usize) -> Result<()> {
	println!("{}", "=== SurrealKV Micro-Benchmark ===".bold().cyan());
	println!("Path:        {}", path.display().to_string().yellow());
	println!("Keys:        {keys}");
	println!("Concurrency: {concurrency}");

	let tree = Arc::new(
		TreeBuilder::new()
			.with_path(path.to_path_buf())
			.build()
			.context("Failed to open database for benchmark")?,
	);

	let start = Instant::now();
	let mut join_set = tokio::task::JoinSet::new();
	let keys_per_worker = keys / concurrency;

	// Benchmark writes
	println!("\n{}", "Running parallel write benchmark...".bold());
	for worker_id in 0..concurrency {
		let tree_clone = Arc::clone(&tree);
		let start_idx = worker_id * keys_per_worker;
		let end_idx = start_idx + keys_per_worker;

		join_set.spawn(async move {
			for i in start_idx..end_idx {
				let k = format!("bench_key_{i:08}").into_bytes();
				let v = format!("bench_val_{i:08}_payload_padding_data").into_bytes();
				let mut tx = tree_clone.begin().unwrap();
				tx.set(&k, &v).unwrap();
				tx.commit().await.unwrap();
			}
		});
	}

	while let Some(res) = join_set.join_next().await {
		res?;
	}

	let write_elapsed = start.elapsed();
	let write_ops = keys as f64 / write_elapsed.as_secs_f64();
	println!("Writes completed: {} ops in {:.2?} ({:.0} ops/sec)", keys, write_elapsed, write_ops);

	// Benchmark reads
	let read_start = Instant::now();
	for worker_id in 0..concurrency {
		let tree_clone = Arc::clone(&tree);
		let start_idx = worker_id * keys_per_worker;
		let end_idx = start_idx + keys_per_worker;

		join_set.spawn(async move {
			for i in start_idx..end_idx {
				let k = format!("bench_key_{i:08}").into_bytes();
				let tx = tree_clone.begin().unwrap();
				let val = tx.get(&k).unwrap();
				assert!(val.is_some());
			}
		});
	}

	while let Some(res) = join_set.join_next().await {
		res?;
	}

	let read_elapsed = read_start.elapsed();
	let read_ops = keys as f64 / read_elapsed.as_secs_f64();
	println!("Reads completed:  {} ops in {:.2?} ({:.0} ops/sec)", keys, read_elapsed, read_ops);

	let tree_inner = Arc::into_inner(tree).unwrap();
	tree_inner.close().await?;
	Ok(())
}

fn parse_input_bytes(input: &str, hex: bool) -> Result<Vec<u8>> {
	if hex {
		let clean = input.trim_start_matches("0x").trim();
		hex_decode(clean)
	} else {
		Ok(input.as_bytes().to_vec())
	}
}

fn format_bytes(bytes: &[u8], hex: bool) -> String {
	if hex {
		let mut s = String::with_capacity(bytes.len() * 2 + 2);
		s.push_str("0x");
		for b in bytes {
			use std::fmt::Write;
			let _ = write!(s, "{b:02x}");
		}
		s
	} else {
		match std::str::from_utf8(bytes) {
			Ok(valid) => valid.to_string(),
			Err(_) => format_bytes(bytes, true),
		}
	}
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
	if !s.len().is_multiple_of(2) {
		bail!("Hex string must have an even length");
	}
	let mut bytes = Vec::with_capacity(s.len() / 2);
	for i in (0..s.len()).step_by(2) {
		let byte = u8::from_str_radix(&s[i..i + 2], 16)
			.map_err(|e| anyhow::anyhow!("Invalid hex digit at {i}: {e}"))?;
		bytes.push(byte);
	}
	Ok(bytes)
}
