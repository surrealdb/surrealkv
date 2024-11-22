use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "skv",
    about = "SurrealKV CLI Tool",
    version,
    author = "Surreal Authors"
)]
pub struct Cli {
    /// Path to the SurrealKV database file or directory
    #[arg(short, long)]
    pub database: PathBuf,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// List top N keys with most versions
    TopVersions {
        /// Output format (json/table)
        #[arg(short, long, default_value = "table")]
        format: String,
        /// Number of top keys to show
        #[arg(short = 'n', long, default_value = "10")]
        count: usize,
    },
    /// Delete all versions except latest for specified keys
    PruneKeys {
        /// File containing list of keys to prune
        #[arg(short, long, group = "keys_input")]
        keys_file: Option<PathBuf>,
        /// Comma-separated list of keys to prune
        #[arg(short = 'k', long, value_delimiter = ',', group = "keys_input")]
        keys: Option<Vec<String>>,
    },
    /// Delete all versions except latest for all keys
    PruneAll {},
    /// Show database statistics
    Stats,
}
