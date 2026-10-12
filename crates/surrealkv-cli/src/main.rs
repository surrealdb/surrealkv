use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use surrealkv::inspect::{self, CryptoSummary, DbInfo, EncryptionStatus, ManifestInfo, SstInfo};
use surrealkv::{
	CipherPreference,
	CipherSuite,
	EncryptionError,
	KeyManager,
	LSMIterator,
	Options,
	SoftwareKeyManager,
	TreeBuilder,
};
use tabled::settings::Style;
use tabled::{Table as DisplayTable, Tabled};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

/// The environment variable that names the key file, like `--key-file`. It holds a path and never
/// a key: a key in an environment variable or on a command line is visible to every process that
/// can list them, and is inherited by every child process.
const KEY_FILE_ENV: &str = "SKV_KEY_FILE";

/// What an exit status of `enc-info` says: 0 for a directory whose files are consistent, this for
/// one that is not. 1 is for what could not be read at all, and 2 is clap's for a command line
/// that is not one.
const EXIT_INCONSISTENT: u8 = 3;

#[derive(Parser)]
#[command(
	name = "skv",
	author = "SurrealDB Team",
	version,
	about = "Operator and developer inspection, dump, and diagnostic CLI for SurrealKV",
	after_help = "Encrypted databases: a command that opens a database or a file refuses an \
	              encrypted one unless it is given its key, with a message and a non-zero status, \
	              and changes nothing. The key is a file with the 32 byte root key of key id 1, as \
	              32 raw bytes or 64 hex digits: pass it with --key-file <PATH> or set SKV_KEY_FILE \
	              to its path. On Unix the file must not be readable by every user. `enc-info` \
	              needs no key. Encryption is a preview, and a key is only used by a build that \
	              has the `encryption-preview` feature."
)]
struct Cli {
	#[command(subcommand)]
	command: Commands,
}

/// How a command is given the key of an encrypted database.
#[derive(Args, Debug, Clone, Default)]
struct KeyArgs {
	/// File that holds the 32 byte root key of the database, key id 1, as 32 raw bytes or 64 hex
	/// digits. SKV_KEY_FILE names it as well. The key itself is never taken from the command
	/// line or from the environment
	#[arg(long, value_name = "PATH")]
	key_file: Option<PathBuf>,

	/// Accept a key file that every user can read, which is refused on Unix by default
	#[arg(long, default_value_t = false)]
	allow_insecure_key_file: bool,
}

impl KeyArgs {
	/// The key manager that holds the key that `--key-file`, or `SKV_KEY_FILE` if there is no
	/// flag, names, if either does. The key is read here, once, into memory that the manager wipes,
	/// and nothing but the manager holds it.
	fn manager(&self) -> Result<Option<Arc<SoftwareKeyManager>>> {
		let path = match &self.key_file {
			Some(path) => path.clone(),
			None => match std::env::var_os(KEY_FILE_ENV).filter(|value| !value.is_empty()) {
				Some(value) => PathBuf::from(value),
				None => return Ok(None),
			},
		};
		let manager = if self.allow_insecure_key_file {
			SoftwareKeyManager::from_key_file_ignoring_permissions(&path)
		} else {
			SoftwareKeyManager::from_key_file(&path)
		}
		.with_context(|| format!("Failed to load the key file {}", path.display()))?;
		Ok(Some(Arc::new(manager)))
	}
}

/// The options to inspect with: those of a database that has no key, or that are given the key.
fn inspect_options(key: &Option<Arc<SoftwareKeyManager>>) -> Options {
	let options = Options::new();
	match key {
		Some(manager) => options.with_encryption(
			Arc::<SoftwareKeyManager>::clone(manager) as Arc<dyn KeyManager>,
			CipherPreference::Auto,
		),
		None => options,
	}
}

/// A builder for the database at `path`, with its key if there is one.
fn tree_builder(path: &Path, key: &Option<Arc<SoftwareKeyManager>>) -> TreeBuilder {
	let builder = TreeBuilder::new().with_path(path.to_path_buf());
	match key {
		Some(manager) => builder.with_encryption(
			Arc::<SoftwareKeyManager>::clone(manager) as Arc<dyn KeyManager>,
			CipherPreference::Auto,
		),
		None => builder,
	}
}

/// What to do about an error, for an operator, when it is one of an encrypted database.
fn hint_for(error: &anyhow::Error) -> Option<&'static str> {
	for cause in error.chain() {
		match cause.downcast_ref::<surrealkv::Error>() {
			Some(surrealkv::Error::Encryption(EncryptionError::NotConfigured)) => {
				return Some(
					"This is an encrypted database or file. Pass the file that holds its key with \
					 --key-file <PATH>, or set SKV_KEY_FILE to its path. Nothing was changed.",
				);
			}
			Some(surrealkv::Error::Encryption(
				EncryptionError::WrongKey
				| EncryptionError::UnknownKeyId
				| EncryptionError::KeyRetired,
			)) => {
				return Some(
					"The key that was given does not open this database or file. Check which key \
					 file it is: skv holds one key, key id 1. Nothing was changed.",
				);
			}
			Some(surrealkv::Error::Encryption(EncryptionError::NotEncrypted)) => {
				return Some(
					"A key was given for data that is not encrypted: encryption is chosen when a \
					 database is created and is never added to one that exists. Leave out \
					 --key-file and SKV_KEY_FILE for a database that is not encrypted. Nothing \
					 was changed.",
				);
			}
			Some(surrealkv::Error::Encryption(
				EncryptionError::PlaintextInEncryptedDb
				| EncryptionError::DbIdMismatch
				| EncryptionError::SuiteMismatch
				| EncryptionError::FileIdMismatch,
			)) => {
				return Some(
					"A file is not the one that this database or its name says. `skv enc-info \
					 <DIR>` needs no key and lists which files of a database are not consistent. \
					 Nothing was changed.",
				);
			}
			Some(surrealkv::Error::InvalidArgument(message))
				if message.contains("not enabled in this build") =>
			{
				return Some(
					"This build of skv cannot use a key. Encryption is a preview: build skv with \
					 `--features encryption-preview` to read encrypted data. `enc-info` needs no \
					 key.",
				);
			}
			_ => {}
		}
	}
	None
}

#[derive(Subcommand)]
enum Commands {
	/// Inspect database directory and print overall storage health
	Inspect {
		/// Path to database directory
		path: PathBuf,

		#[command(flatten)]
		key_args: KeyArgs,
	},

	/// Say which keys cover which files of a database, without a key
	///
	/// Reads only the headers at the start of the files: whether the database is encrypted, its
	/// cipher suite and identity, and for each key id how many files and bytes of each kind it
	/// covers. Exit status 0 if its files are consistent, 3 if a warning was found, such as a
	/// file in the clear in an encrypted database or a file of another database, and 1 if it
	/// cannot be read.
	EncInfo {
		/// Path to database directory
		path: PathBuf,
	},

	/// Dump and analyze LevelManifest state and SSTable hierarchy
	Manifest {
		/// Path to database directory or MANIFEST file
		path: PathBuf,

		#[command(flatten)]
		key_args: KeyArgs,
	},

	/// Dump and verify an individual SSTable (.sst file)
	Sst {
		/// Path to .sst file
		path: PathBuf,

		/// Dump all key-value entries
		#[arg(long, default_value_t = false)]
		dump_keys: bool,

		/// Recalculate and verify the checksum of every block, of every kind, and with a key
		/// authenticate the blocks of an encrypted table
		#[arg(long, default_value_t = false)]
		verify_checksum: bool,

		/// Display binary keys and values in hexadecimal format
		#[arg(long, default_value_t = false)]
		hex: bool,

		/// Only count entries without displaying details
		#[arg(long, default_value_t = false)]
		count_only: bool,

		#[command(flatten)]
		key_args: KeyArgs,
	},

	/// Dump and decode WAL segments
	Wal {
		/// Path to database directory or wal/ directory
		path: PathBuf,

		#[command(flatten)]
		key_args: KeyArgs,
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

		#[command(flatten)]
		key_args: KeyArgs,
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

		#[command(flatten)]
		key_args: KeyArgs,
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

		#[command(flatten)]
		key_args: KeyArgs,
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

		#[command(flatten)]
		key_args: KeyArgs,
	},

	/// Verify the checksum of every block of every SSTable in the database
	///
	/// With a key, the blocks of an encrypted table are authenticated as well. Without one a
	/// sealed table is reported as not verified, never as corrupt.
	Scrub {
		/// Path to database directory
		path: PathBuf,

		#[command(flatten)]
		key_args: KeyArgs,
	},

	/// Explicitly migrate a RocksDB or SurrealKV V1 store to V2
	///
	/// An encrypted destination is refused: a migration writes without a key.
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

		#[command(flatten)]
		key_args: KeyArgs,
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

#[derive(Tabled)]
struct SealedWalFileRow {
	#[tabled(rename = "WAL Segment File")]
	file_name: String,
	#[tabled(rename = "Size (Bytes)")]
	size: u64,
	#[tabled(rename = "Key ID")]
	key_id: String,
	#[tabled(rename = "Batches")]
	batches: String,
	#[tabled(rename = "Torn")]
	torn: String,
}

#[tokio::main]
async fn main() -> ExitCode {
	tracing_subscriber::registry().with(fmt::layer()).with(EnvFilter::from_default_env()).init();

	let cli = Cli::parse();
	match run(cli).await {
		Ok(code) => code,
		Err(error) => {
			// What the default of a `main` that returns an error prints, and a hint when the error
			// is one of an encrypted database.
			eprintln!("Error: {error:?}");
			if let Some(hint) = hint_for(&error) {
				eprintln!("\n{hint}");
			}
			ExitCode::FAILURE
		}
	}
}

async fn run(cli: Cli) -> Result<ExitCode> {
	match cli.command {
		Commands::Inspect {
			path,
			key_args,
		} => {
			cmd_inspect(&path, &key_args)?;
		}
		Commands::EncInfo {
			path,
		} => {
			return cmd_enc_info(&path);
		}
		Commands::Manifest {
			path,
			key_args,
		} => {
			cmd_manifest(&path, &key_args)?;
		}
		Commands::Sst {
			path,
			dump_keys,
			verify_checksum,
			hex,
			count_only,
			key_args,
		} => {
			cmd_sst(&path, dump_keys, verify_checksum, hex, count_only, &key_args)?;
		}
		Commands::Wal {
			path,
			key_args,
		} => {
			cmd_wal(&path, &key_args)?;
		}
		Commands::Get {
			path,
			key,
			hex,
			key_args,
		} => {
			return cmd_get(&path, &key, hex, &key_args).await;
		}
		Commands::Scan {
			path,
			start,
			end,
			prefix,
			limit,
			keys_only,
			hex,
			key_args,
		} => {
			cmd_scan(&path, start, end, prefix, limit, keys_only, hex, &key_args).await?;
		}
		Commands::Put {
			path,
			key,
			value,
			hex,
			key_args,
		} => {
			cmd_put(&path, &key, &value, hex, &key_args).await?;
		}
		Commands::Delete {
			path,
			key,
			hex,
			key_args,
		} => {
			cmd_delete(&path, &key, hex, &key_args).await?;
		}
		Commands::Scrub {
			path,
			key_args,
		} => {
			return cmd_scrub(&path, &key_args);
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
			key_args,
		} => {
			cmd_bench(&path, keys, concurrency, &key_args).await?;
		}
	}

	Ok(ExitCode::SUCCESS)
}

/// How a suite is called.
fn suite_name(suite: CipherSuite) -> String {
	match suite {
		CipherSuite::Aes256Gcm => "AES-256-GCM".to_string(),
		CipherSuite::XChaCha20Poly1305 => "XChaCha20-Poly1305".to_string(),
		other => format!("{other:?}"),
	}
}

/// What the header of an encrypted file says, in a line.
fn encryption_line(header: &CryptoSummary) -> String {
	format!("{}, key id {}, database {}", suite_name(header.suite), header.key_id, header.db_id)
}

fn cmd_inspect(path: &Path, key_args: &KeyArgs) -> Result<()> {
	println!("{}", "=== SurrealKV Database Inspection ===".bold().cyan());
	println!("Path: {}", path.display().to_string().yellow());

	let key = key_args.manager()?;
	let info: DbInfo = inspect::inspect_db_with_options(path, &inspect_options(&key))
		.with_context(|| format!("Failed to inspect database at {}", path.display()))?;
	if let Some(header) = &info.encryption {
		println!("Encryption:       {}", encryption_line(header));
	}

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

fn cmd_manifest(path: &Path, key_args: &KeyArgs) -> Result<()> {
	println!("{}", "=== LevelManifest State ===".bold().cyan());
	let key = key_args.manager()?;
	let info: ManifestInfo = inspect::dump_manifest_with_options(path, &inspect_options(&key))
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
	key_args: &KeyArgs,
) -> Result<()> {
	println!("{}", "=== SSTable File Inspection ===".bold().cyan());
	println!("File: {}", path.display().to_string().yellow());

	let key = key_args.manager()?;
	let info: SstInfo = inspect::dump_sst_with_options(
		path,
		dump_keys || count_only,
		verify_checksum,
		&inspect_options(&key),
	)
	.with_context(|| format!("Failed to inspect SSTable at {}", path.display()))?;
	if let Some(id) = info.table_id {
		println!("Table ID:           {id}");
	}
	if let Some(header) = &info.encryption {
		println!("Encryption:         {}", encryption_line(header));
	}

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

fn cmd_wal(path: &Path, key_args: &KeyArgs) -> Result<()> {
	println!("{}", "=== WAL Segments Inspection ===".bold().cyan());
	let key = key_args.manager()?;
	let info = inspect::dump_wal_with_options(path, &inspect_options(&key))
		.with_context(|| format!("Failed to inspect WAL at {}", path.display()))?;

	println!(
		"Total WAL Size: {:.2} KB ({} files)",
		info.total_bytes as f64 / 1024.0,
		info.segment_files.len()
	);

	if info.segment_files.is_empty() {
		println!("\n{}", "No WAL segments found.".yellow());
	} else if info.segment_files.iter().any(|s| s.encryption.is_some()) {
		// A log that is sealed also says which key each segment is under, and how many batches
		// were read from it with the key.
		let rows: Vec<SealedWalFileRow> = info
			.segment_files
			.into_iter()
			.map(|s| SealedWalFileRow {
				file_name: s.file_name,
				size: s.file_size,
				key_id: s.encryption.map_or_else(|| "-".to_string(), |e| e.key_id.to_string()),
				batches: s.batches.map_or_else(|| "-".to_string(), |b| b.to_string()),
				torn: if s.torn {
					"yes".to_string()
				} else {
					"no".to_string()
				},
			})
			.collect();
		let table = DisplayTable::new(rows).with(Style::rounded()).to_string();
		println!("{table}");
	} else {
		let rows: Vec<WalFileRow> = info
			.segment_files
			.into_iter()
			.map(|s| WalFileRow {
				file_name: s.file_name,
				size: s.file_size,
				size_kb: format!("{:.2}", s.file_size as f64 / 1024.0),
			})
			.collect();
		let table = DisplayTable::new(rows).with(Style::rounded()).to_string();
		println!("{table}");
	}

	Ok(())
}

async fn cmd_get(path: &Path, key_str: &str, hex: bool, key_args: &KeyArgs) -> Result<ExitCode> {
	let key_bytes = parse_input_bytes(key_str, hex)?;
	let key = key_args.manager()?;
	let tree = tree_builder(path, &key).build().context("Failed to open database")?;

	let tx = tree.begin()?;
	let val = tx.get(&key_bytes)?;
	drop(tx);

	// The status is returned and not exited with, so that the tree is closed, and the key that it
	// holds is wiped, on the way out.
	let found = match val {
		Some(v) => {
			println!("{}", format_bytes(&v, hex));
			true
		}
		None => {
			eprintln!("{}", "(not found)".yellow());
			false
		}
	};

	tree.close().await?;
	Ok(if found {
		ExitCode::SUCCESS
	} else {
		ExitCode::FAILURE
	})
}

#[allow(clippy::too_many_arguments)]
async fn cmd_scan(
	path: &Path,
	start: Option<String>,
	end: Option<String>,
	prefix: Option<String>,
	limit: usize,
	keys_only: bool,
	hex: bool,
	key_args: &KeyArgs,
) -> Result<()> {
	let key = key_args.manager()?;
	let tree = tree_builder(path, &key).build().context("Failed to open database")?;

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

async fn cmd_put(
	path: &Path,
	key_str: &str,
	val_str: &str,
	hex: bool,
	key_args: &KeyArgs,
) -> Result<()> {
	let key_bytes = parse_input_bytes(key_str, hex)?;
	let val_bytes = parse_input_bytes(val_str, hex)?;

	let key = key_args.manager()?;
	let tree = tree_builder(path, &key).build().context("Failed to open database")?;

	let mut tx = tree.begin()?;
	tx.set(&key_bytes, &val_bytes)?;
	tx.commit().await?;

	println!("{}", "OK".green().bold());
	tree.close().await?;
	Ok(())
}

async fn cmd_delete(path: &Path, key_str: &str, hex: bool, key_args: &KeyArgs) -> Result<()> {
	let key_bytes = parse_input_bytes(key_str, hex)?;

	let key = key_args.manager()?;
	let tree = tree_builder(path, &key).build().context("Failed to open database")?;

	let mut tx = tree.begin()?;
	tx.delete(&key_bytes)?;
	tx.commit().await?;

	println!("{}", "OK".green().bold());
	tree.close().await?;
	Ok(())
}

fn cmd_scrub(path: &Path, key_args: &KeyArgs) -> Result<ExitCode> {
	println!("{}", "=== SurrealKV Checksum Integrity Scrub ===".bold().cyan());
	println!("Database: {}", path.display().to_string().yellow());

	let key = key_args.manager()?;
	let report = inspect::scrub_db_with_options(path, &inspect_options(&key))
		.with_context(|| format!("Failed to scrub database at {}", path.display()))?;
	// A database that is encrypted, and was looked at without a key, has not been verified,
	// whatever its tables are or are not: it may hold none, and its log and value log are not
	// tables.
	let unread = key.is_none() && inspect::is_encrypted(path).unwrap_or(false);

	println!("Total SSTables:    {}", report.total_tables);
	println!("Verified SSTables: {}", report.verified_tables);
	if report.not_verified_tables > 0 {
		println!("Not verified:      {} SSTables", report.not_verified_tables);
	}
	println!("Verified Blocks:   {}", report.total_blocks);
	let blocks = &report.blocks;
	println!(
		"  data {}, index partitions {}, top-level index {}, filter partitions {}, filter index {}, meta-index {}",
		blocks.data,
		blocks.index_partitions,
		blocks.top_level_index,
		blocks.filter_partitions,
		blocks.filter_index,
		blocks.meta_index
	);

	if report.corrupt_blocks == 0 && report.errors.is_empty() && !unread {
		println!(
			"\n{} All {} blocks in {} tables verified without corruption.",
			"SCRUB PASSED:".green().bold(),
			report.total_blocks,
			report.verified_tables
		);
		return Ok(ExitCode::SUCCESS);
	}

	if report.corrupt_blocks == 0 {
		// Nothing was found damaged: some tables could not be read, which is for the errors to
		// say.
		eprintln!("\n{} Some tables could not be verified.", "SCRUB INCOMPLETE:".red().bold());
		if unread {
			eprintln!(
				"  - the database is encrypted and no key was given, so nothing of it was verified: \
				 pass --key-file <PATH> or set {KEY_FILE_ENV}"
			);
		}
	} else {
		eprintln!(
			"\n{} Found damage in {} tables!",
			"SCRUB FAILED:".red().bold(),
			report.corrupt_blocks
		);
	}
	for err in report.errors {
		eprintln!("  - {}", err.red());
	}
	Ok(ExitCode::FAILURE)
}

async fn cmd_migrate(source: &Path, destination: &Path) -> Result<()> {
	println!("{}", "=== SurrealKV Offline Migration Tool ===".bold().cyan());
	println!("Source:      {}", source.display().to_string().yellow());
	println!("Destination: {}", destination.display().to_string().green());

	// A migration writes without a key, and the destination of a database that is encrypted is not
	// one that it may be written into. Said before anything of the source is looked at, which can
	// take long.
	if destination.exists() && inspect::is_encrypted(destination).unwrap_or(false) {
		bail!(
			"The destination {} is an encrypted database. A migration writes without a key and is \
			 never done into an encrypted database: choose an empty directory",
			destination.display()
		);
	}

	let is_rocksdb = surrealkv_compat_rocksdb::is_rocksdb_dir(source);
	let is_v1 = surrealkv_compat_v1::is_v1_dir(source);
	let is_indxdb = surrealkv_compat_indxdb::is_indxdb_available(&source.to_string_lossy());

	if !is_rocksdb && !is_v1 && !is_indxdb {
		bail!(
			"Source directory at {} is not a recognized RocksDB, SurrealKV V1, or IndexedDB database",
			source.display()
		);
	}

	let format_name = if is_rocksdb {
		"RocksDB BlockBasedTable"
	} else if is_v1 {
		"SurrealKV V1"
	} else {
		"IndexedDB Store / Dump"
	};
	println!("Detected format: {}", format_name.bold().cyan());

	let records = if is_rocksdb {
		surrealkv_compat_rocksdb::read_all_latest(source)?
	} else if is_v1 {
		surrealkv_compat_v1::read_all_latest(source)?
	} else {
		surrealkv_compat_indxdb::read_all_latest(&source.to_string_lossy()).await?
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

async fn cmd_bench(path: &Path, keys: usize, concurrency: usize, key_args: &KeyArgs) -> Result<()> {
	println!("{}", "=== SurrealKV Micro-Benchmark ===".bold().cyan());
	println!("Path:        {}", path.display().to_string().yellow());
	println!("Keys:        {keys}");
	println!("Concurrency: {concurrency}");

	let key = key_args.manager()?;
	let tree = Arc::new(
		tree_builder(path, &key).build().context("Failed to open database for benchmark")?,
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

fn cmd_enc_info(path: &Path) -> Result<ExitCode> {
	println!("{}", "=== SurrealKV Encryption Info ===".bold().cyan());
	println!("Path: {}", path.display().to_string().yellow());

	let report = inspect::encryption_report(path).with_context(|| {
		format!("Failed to read the encryption of the database at {}", path.display())
	})?;
	print_encryption_report(&report);

	if report.is_consistent() {
		Ok(ExitCode::SUCCESS)
	} else {
		eprintln!(
			"\n{} The files of this database are not consistent: {} warning(s).",
			"INCONSISTENT:".red().bold(),
			report.warnings.len()
		);
		Ok(ExitCode::from(EXIT_INCONSISTENT))
	}
}

/// The files and bytes of a kind of file, in a phrase.
fn totals_phrase(totals: &inspect::FileTotals) -> String {
	format!("{} file(s), {} bytes", totals.files, totals.bytes)
}

fn print_encryption_report(report: &EncryptionStatus) {
	if !report.encrypted {
		println!("Encrypted:        {}", "no".green());
	} else {
		println!("Encrypted:        {}", "yes".green().bold());
		if let Some(suite) = report.suite {
			println!("Cipher suite:     {}", suite_name(suite));
		}
		if let Some(db_id) = &report.db_id {
			println!("Database id:      {db_id}");
		}
		if let Some(key_id) = report.manifest_key_id {
			println!("Manifest key id:  {key_id}");
		}
	}

	if !report.keys.is_empty() {
		println!("\n{}", "Files under each key id:".bold());
		for usage in &report.keys {
			println!("Key {}:", usage.key_id);
			println!("  manifest: {}", totals_phrase(&usage.manifest));
			println!("  tables: {}", totals_phrase(&usage.tables));
			println!("  value log: {}", totals_phrase(&usage.value_log));
			println!("  WAL: {}", totals_phrase(&usage.wal));
		}
		println!(
			"A key may be retired only when it covers no file here, is not the key of the \
			 manifest, and no checkpoint or backup needs it."
		);
	}
	if report.unattributed.files > 0 {
		println!(
			"Files whose header could not be read, or that are empty or cut short: {}",
			totals_phrase(&report.unattributed)
		);
	}
	if report.plaintext.files > 0 {
		println!(
			"Files in the clear in an encrypted database: {}",
			totals_phrase(&report.plaintext)
		);
	}
	if !report.notes.is_empty() {
		println!("\n{}", "Notes:".bold());
		for note in &report.notes {
			println!("  - {note}");
		}
	}
	if !report.warnings.is_empty() {
		println!("\n{}", "Warnings:".bold());
		for warning in &report.warnings {
			println!("  {} {warning}", "WARNING:".yellow().bold());
		}
	}
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
	// Decoded as bytes: a string that holds a character of more than one byte is not hex, and
	// cutting it in pairs of bytes would cut it inside a character.
	let (pairs, odd) = s.as_bytes().as_chunks::<2>();
	if !odd.is_empty() {
		bail!("Hex string must have an even length");
	}
	let mut bytes = Vec::with_capacity(pairs.len());
	for (i, &[high, low]) in pairs.iter().enumerate() {
		let (Some(high), Some(low)) = ((high as char).to_digit(16), (low as char).to_digit(16))
		else {
			bail!("Invalid hex digit at {}", i * 2);
		};
		bytes.push(((high << 4) | low) as u8);
	}
	Ok(bytes)
}
