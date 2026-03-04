use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::RecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::array::iter::{ArrayIteratorAdapter, ArrayIteratorExt};
use vortex::dtype::DType;
use vortex::dtype::arrow::FromArrowType;
use vortex::error::VortexError;
use vortex::file::{WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

use crate::collect_parquet_files;
use crate::compressor::Utf8Compressor;

async fn convert_one(path: PathBuf, output_path: PathBuf) -> Result<()> {
    println!("Converting: {}", path.display());

    let file = File::open(&path).with_context(|| format!("failed to open: {}", path.display()))?;
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
        .with_strategy(
            WriteStrategyBuilder::default()
                .with_compressor(Utf8Compressor::default())
                .build(),
        )
        .write(&mut f, array_iter.into_array_stream())
        .await?;

    println!("  -> {}", output_path.display());
    Ok(())
}

pub async fn to_vortex(input_dir: &Path, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)?;

    let files = collect_parquet_files(input_dir)?;
    println!(
        "Found {} parquet files to convert. Processing with {} threads...",
        files.len(),
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    );

    let concurrency = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

    let mut handles = Vec::with_capacity(files.len());
    for path in files {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let output_path = output_dir
            .join(path.file_stem().unwrap())
            .with_extension("vortex");

        handles.push(tokio::spawn(async move {
            let result = convert_one(path, output_path).await;
            drop(permit);
            result
        }));
    }

    for handle in handles {
        handle.await??;
    }

    println!("Done.");
    Ok(())
}
