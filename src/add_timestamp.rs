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

    // Phase 2: Assign each file to its own 1-hour bucket [hour_start, hour_start + 1h)
    // Base: 2026-02-01 00:00:00 UTC = 1769904000 seconds since epoch = 1769904000000000 us
    const BASE_TS_US: i64 = 1_769_904_000_000_000;
    const ONE_HOUR_US: i64 = 3_600_000_000; // 1 hour in microseconds

    // For each file: hour_start = BASE + i * ONE_HOUR, step = ONE_HOUR / row_count
    // Timestamps: hour_start + j * step for j in 0..row_count
    // All timestamps stay within [hour_start, hour_start + ONE_HOUR)
    struct FileTimestampInfo {
        hour_start: i64,
        step_us: i64,
    }
    let mut ts_infos: Vec<FileTimestampInfo> = Vec::with_capacity(files.len());
    let mut total_rows: i64 = 0;
    for (i, &count) in row_counts.iter().enumerate() {
        let hour_start = BASE_TS_US + i as i64 * ONE_HOUR_US;
        let step_us = if count <= 1 { 0 } else { ONE_HOUR_US / count };
        ts_infos.push(FileTimestampInfo {
            hour_start,
            step_us,
        });
        total_rows += count;
    }

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
        let hour_start = ts_infos[i].hour_start;
        let step_us = ts_infos[i].step_us;

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

            let writer_props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_column_dictionary_enabled("_timestamp".into(), false)
                .set_column_encoding("_timestamp".into(), Encoding::DELTA_BINARY_PACKED)
                .build();

            let mut writer =
                ArrowWriter::try_new(out_file, new_schema.clone(), Some(writer_props))?;

            let mut row_offset: i64 = 0;
            for batch_result in reader {
                let batch = batch_result?;
                let num_rows = batch.num_rows();

                // Timestamps: hour_start + j * step_us, ascending within [hour_start, hour_start + 1h)
                let timestamps: Vec<i64> = (0..num_rows as i64)
                    .map(|j| hour_start + (row_offset + j) * step_us)
                    .collect();
                row_offset += num_rows as i64;

                let ts_array = Int64Array::from(timestamps);

                let mut columns: Vec<Arc<dyn arrow::array::Array>> = batch.columns().to_vec();
                columns.push(Arc::new(ts_array));

                let new_batch = RecordBatch::try_new(new_schema.clone(), columns)?;
                writer.write(&new_batch)?;
            }

            writer.close()?;
            let last_ts = hour_start + (row_offset - 1).max(0) * step_us;
            println!(
                "  -> {} (hour bucket: {}..{}, timestamp range: {}..{})",
                output_path.display(),
                hour_start,
                hour_start + ONE_HOUR_US,
                hour_start,
                last_ts
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
