mod add_timestamp;
mod compressor;
mod to_vortex;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "clickbench-convert")]
#[command(about = "CLI tool for ClickBench dataset conversion")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Add an auto-incrementing _timestamp (i64) column to parquet files
    AddTimestamp {
        /// Input directory containing parquet files
        #[arg(short, long)]
        input: PathBuf,
        /// Output directory for parquet files with _timestamp column
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Convert parquet files to vortex format
    ToVortex {
        /// Input directory containing parquet files
        #[arg(short, long)]
        input: PathBuf,
        /// Output directory for vortex files
        #[arg(short, long)]
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::AddTimestamp { input, output } => {
            add_timestamp::add_timestamp(&input, &output).await?;
        }
        Commands::ToVortex { input, output } => {
            to_vortex::to_vortex(&input, &output).await?;
        }
    }

    Ok(())
}

pub(crate) fn collect_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory: {}", dir.display()))?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    files.sort();
    Ok(files)
}
