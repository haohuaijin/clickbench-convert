use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::RecordBatchReader;
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use clap::{Parser, Subcommand};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::array::iter::{ArrayIteratorAdapter, ArrayIteratorExt};
use vortex::dtype::DType;
use vortex::dtype::arrow::FromArrowType;
use vortex::error::VortexError;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

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
            add_timestamp(&input, &output).await?;
        }
        Commands::ToVortex { input, output } => {
            to_vortex(&input, &output).await?;
        }
    }

    Ok(())
}

fn collect_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
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

async fn add_timestamp(input_dir: &Path, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)?;

    let files = collect_parquet_files(input_dir)?;
    println!("Found {} parquet files", files.len());

    // Phase 1: Read metadata to get row counts (fast, no data reading)
    let mut row_counts: Vec<i64> = Vec::with_capacity(files.len());
    for path in &files {
        let file =
            File::open(path).with_context(|| format!("failed to open: {}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        row_counts.push(builder.metadata().file_metadata().num_rows());
    }

    // Phase 2: Compute starting timestamps via prefix sum (file name order)
    let mut start_timestamps: Vec<i64> = Vec::with_capacity(files.len());
    let mut cumulative: i64 = 0;
    for &count in &row_counts {
        start_timestamps.push(cumulative);
        cumulative += count;
    }
    let total_rows = cumulative;

    println!(
        "Total rows across all files: {}. Processing with {} threads...",
        total_rows,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    );

    // Phase 3: Process files in parallel
    let concurrency = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let output_dir = output_dir.to_path_buf();

    let mut handles = Vec::with_capacity(files.len());
    for (i, path) in files.into_iter().enumerate() {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let output_dir = output_dir.clone();
        let start_ts = start_timestamps[i];

        handles.push(tokio::task::spawn_blocking(move || -> Result<()> {
            let _permit = permit;
            let filename = path.file_name().unwrap().to_owned();
            let output_path = output_dir.join(&filename);

            println!("Processing: {}", path.display());

            let file =
                File::open(&path).with_context(|| format!("failed to open: {}", path.display()))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

            let original_schema = reader.schema();
            let mut fields: Vec<Arc<Field>> = original_schema.fields().iter().cloned().collect();
            fields.push(Arc::new(Field::new("_timestamp", DataType::Int64, false)));
            let new_schema = Arc::new(Schema::new(fields));

            let out_file = File::create(&output_path)
                .with_context(|| format!("failed to create: {}", output_path.display()))?;
            let mut writer = ArrowWriter::try_new(out_file, new_schema.clone(), None)?;

            let mut timestamp = start_ts;
            for batch_result in reader {
                let batch = batch_result?;
                let num_rows = batch.num_rows();

                let timestamps: Vec<i64> = (timestamp..timestamp + num_rows as i64).collect();
                timestamp += num_rows as i64;

                let ts_array = Int64Array::from(timestamps);

                let mut columns: Vec<Arc<dyn arrow::array::Array>> = batch.columns().to_vec();
                columns.push(Arc::new(ts_array));

                let new_batch = RecordBatch::try_new(new_schema.clone(), columns)?;
                writer.write(&new_batch)?;
            }

            writer.close()?;
            println!(
                "  -> {} (timestamp range: {}..{})",
                output_path.display(),
                start_ts,
                timestamp
            );
            Ok(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    println!("Done. Total rows: {}", total_rows);
    Ok(())
}

async fn to_vortex(input_dir: &Path, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)?;

    let files = collect_parquet_files(input_dir)?;
    println!("Found {} parquet files to convert", files.len());

    for path in &files {
        let filename = path.file_stem().unwrap();
        let output_path = output_dir.join(filename).with_extension("vortex");

        println!("Converting: {}", path.display());

        let file =
            File::open(path).with_context(|| format!("failed to open: {}", path.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

        let array_iter = ArrayIteratorAdapter::new(
            DType::from_arrow(reader.schema()),
            reader.map(|br| {
                br.map_err(VortexError::from)
                    .and_then(|b| ArrayRef::from_arrow(b, false))
            }),
        );

        let mut f = tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&output_path)
            .await?;

        let session = VortexSession::default().with_tokio();
        session
            .write_options()
            .write(&mut f, array_iter.into_array_stream())
            .await?;

        println!("  -> {}", output_path.display());
    }

    println!("Done.");
    Ok(())
}
