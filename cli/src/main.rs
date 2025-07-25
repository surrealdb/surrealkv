use anyhow::Result;
use clap::Parser;
use std::process;

mod analyzer;
mod commands;

use analyzer::Analyzer;
use commands::{Cli, Commands};

fn main() {
    if let Err(err) = run() {
        eprintln!("Error: {err}");
        process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let analyzer = Analyzer::new(cli.database)?;

    match &cli.command {
        Commands::TopVersions { format, count } => {
            analyzer.analyze_top_versions(format, *count)?;
        }
        Commands::PruneKeys {
            keys_file,
            keys,
            destination,
        } => {
            analyzer.prune_specific_keys(keys_file.as_ref(), keys.as_ref(), destination)?;
        }
        Commands::PruneAll { destination } => {
            analyzer.prune_all_keys(destination)?;
        }
        Commands::Stats => {
            let stats = analyzer.collect_stats()?;
            stats.display();
        }
        Commands::Repair => {
            analyzer.repair()?;
        }
    }

    Ok(())
}
