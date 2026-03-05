use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::RecordBatchReader;
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::basic::Encoding;
use parquet::file::properties::WriterProperties;

use crate::collect_parquet_files;

pub async fn add_timestamp(input_dir: &Path, output_dir: &Path) -> Result<()> {
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
    // Base: 2026-02-01 00:00:00 UTC = 1769904000 seconds since epoch = 1769904000000000 us
    const BASE_TS_US: i64 = 1_769_904_000_000_000;
    const STEP_US: i64 = 10_000;

    let mut start_timestamps: Vec<i64> = Vec::with_capacity(files.len());
    let mut cumulative: i64 = 0;
    for &count in &row_counts {
        // Each file's first row gets the highest timestamp in its range
        start_timestamps.push(BASE_TS_US + (cumulative + count - 1) * STEP_US);
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
            fields.push(Arc::new(Field::new(
                "_timestamp",
                DataType::Int64,
                false,
            )));
            let new_schema = Arc::new(Schema::new(fields));

            let out_file = File::create(&output_path)
                .with_context(|| format!("failed to create: {}", output_path.display()))?;

            let writer_props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_column_dictionary_enabled("_timestamp".into(), false)
                .set_column_encoding("_timestamp".into(), Encoding::DELTA_BINARY_PACKED)
                .build();

            let mut writer =
                ArrowWriter::try_new(out_file, new_schema.clone(), Some(writer_props))?;

            let mut timestamp = start_ts;
            for batch_result in reader {
                let batch = batch_result?;
                let num_rows = batch.num_rows();

                let timestamps: Vec<i64> = (0..num_rows as i64)
                    .map(|i| timestamp - i * STEP_US)
                    .collect();
                timestamp -= num_rows as i64 * STEP_US;

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
