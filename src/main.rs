mod add_timestamp;
mod compressor;
mod register_file_list;
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
    /// Register parquet/vortex files into OpenObserve's file_list SQLite table
    RegisterFileList {
        /// Path to the SQLite database file
        #[arg(long)]
        db: PathBuf,
        /// Input directory containing parquet or vortex files
        #[arg(short, long)]
        input: PathBuf,
        /// OpenObserve data directory (e.g., ./data/openobserve/). Files will be copied to {data-dir}/stream/files/...
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Stream name (e.g., "hits")
        #[arg(long)]
        stream_name: String,
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
        Commands::RegisterFileList {
            db,
            input,
            data_dir,
            stream_name,
        } => {
            register_file_list::register_file_list(&db, &input, data_dir.as_deref(), &stream_name)
                .await?;
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
    files.sort_by(|a, b| {
        let num = |p: &PathBuf| -> Option<i64> {
            p.file_stem()?.to_str()?.rsplit('_').next()?.parse().ok()
        };
        match (num(a), num(b)) {
            (Some(na), Some(nb)) => na.cmp(&nb),
            _ => a.cmp(b),
        }
    });
    Ok(files)
}
