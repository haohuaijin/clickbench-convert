use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::TimeZone;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use rusqlite::Connection;

struct FileMeta {
    min_ts: i64,
    max_ts: i64,
    records: i64,
    original_size: i64,
    compressed_size: i64,
}

fn read_parquet_metadata(path: &Path) -> Result<FileMeta> {
    let file =
        std::fs::File::open(path).with_context(|| format!("failed to open: {}", path.display()))?;
    let compressed_size = file.metadata()?.len() as i64;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();

    // Try key-value metadata first (OpenObserve format)
    if let Some(kv_metadata) = metadata.file_metadata().key_value_metadata() {
        let mut meta = FileMeta {
            min_ts: 0,
            max_ts: 0,
            records: 0,
            original_size: 0,
            compressed_size,
        };
        let mut found = false;
        for kv in kv_metadata {
            match kv.key.as_str() {
                "min_ts" => {
                    meta.min_ts = kv.value.as_ref().unwrap().parse()?;
                    found = true;
                }
                "max_ts" => {
                    meta.max_ts = kv.value.as_ref().unwrap().parse()?;
                    found = true;
                }
                "records" => {
                    meta.records = kv.value.as_ref().unwrap().parse()?;
                    found = true;
                }
                "original_size" => {
                    meta.original_size = kv.value.as_ref().unwrap().parse()?;
                    found = true;
                }
                _ => {}
            }
        }
        if found && meta.min_ts > 0 && meta.max_ts > 0 {
            return Ok(meta);
        }
    }

    // Fall back to row group statistics
    let schema_descr = metadata.file_metadata().schema_descr();
    let ts_col_idx = schema_descr
        .columns()
        .iter()
        .position(|c| c.name() == "_timestamp");

    let mut records: i64 = 0;
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut original_size: i64 = 0;

    for rg_idx in 0..metadata.num_row_groups() {
        let rg = metadata.row_group(rg_idx);
        records += rg.num_rows();
        original_size += rg.total_byte_size();

        if let Some(idx) = ts_col_idx {
            let col_meta = rg.column(idx);
            if let Some(stats) = col_meta.statistics()
                && let Statistics::Int64(s) = stats
            {
                if let Some(&min) = s.min_opt() {
                    min_ts = min_ts.min(min);
                }
                if let Some(&max) = s.max_opt() {
                    max_ts = max_ts.max(max);
                }
            }
        }
    }

    if ts_col_idx.is_none() || min_ts == i64::MAX {
        bail!(
            "no _timestamp column or statistics found in {}",
            path.display()
        );
    }

    Ok(FileMeta {
        min_ts,
        max_ts,
        records,
        original_size,
        compressed_size,
    })
}

fn date_key_from_ts(ts_us: i64) -> String {
    let dt = chrono::Utc
        .timestamp_opt(ts_us / 1_000_000, ((ts_us % 1_000_000) * 1000) as u32)
        .single()
        .unwrap_or_default();
    dt.format("%Y/%m/%d/%H").to_string()
}

fn collect_files(dir: &Path, extensions: &[&str]) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory: {}", dir.display()))?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            let ext = path.extension().and_then(|s| s.to_str())?;
            if extensions.contains(&ext) {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    files.sort();
    Ok(files)
}

pub fn register_file_list(
    db_path: &Path,
    input_dir: &Path,
    org: &str,
    stream_type: &str,
    stream_name: &str,
    account: &str,
    parquet_metadata_dir: Option<&Path>,
) -> Result<()> {
    let files = collect_files(input_dir, &["parquet", "vortex"])?;
    if files.is_empty() {
        bail!(
            "no parquet or vortex files found in {}",
            input_dir.display()
        );
    }

    println!("Found {} files to register", files.len());

    let conn = Connection::open(db_path)
        .with_context(|| format!("failed to open database: {}", db_path.display()))?;

    // Create file_list table if not exists (matching OpenObserve schema)
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS file_list (
            id              INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            account         VARCHAR NOT NULL,
            org             VARCHAR NOT NULL,
            stream          VARCHAR NOT NULL,
            date            VARCHAR NOT NULL,
            file            VARCHAR NOT NULL,
            deleted         BOOLEAN DEFAULT FALSE NOT NULL,
            flattened       BOOLEAN DEFAULT FALSE NOT NULL,
            min_ts          BIGINT NOT NULL,
            max_ts          BIGINT NOT NULL,
            records         BIGINT NOT NULL,
            original_size   BIGINT NOT NULL,
            compressed_size BIGINT NOT NULL,
            index_size      BIGINT NOT NULL,
            updated_at      BIGINT NOT NULL
        );",
    )?;

    let stream_key = format!("{org}/{stream_type}/{stream_name}");
    let now_us = chrono::Utc::now().timestamp_micros();

    let mut inserted = 0;
    let mut skipped = 0;

    for path in &files {
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .context("invalid filename")?;

        let (meta, registered_filename) = match ext {
            "parquet" => {
                let meta = read_parquet_metadata(path)
                    .with_context(|| format!("failed to read metadata: {}", path.display()))?;
                (meta, filename.to_string())
            }
            "vortex" => {
                // For vortex files, read metadata from corresponding parquet file
                let parquet_dir = parquet_metadata_dir.unwrap_or(input_dir);
                let parquet_name = format!(
                    "{}.parquet",
                    path.file_stem().and_then(|s| s.to_str()).unwrap()
                );
                let parquet_path = parquet_dir.join(&parquet_name);

                if !parquet_path.exists() {
                    println!(
                        "  SKIP: {} (no corresponding parquet file at {})",
                        filename,
                        parquet_path.display()
                    );
                    skipped += 1;
                    continue;
                }

                let mut meta = read_parquet_metadata(&parquet_path).with_context(|| {
                    format!(
                        "failed to read parquet metadata: {}",
                        parquet_path.display()
                    )
                })?;
                // Use actual vortex file size as compressed_size
                meta.compressed_size = std::fs::metadata(path)?.len() as i64;
                (meta, filename.to_string())
            }
            _ => continue,
        };

        let date_key = date_key_from_ts(meta.min_ts);

        // Construct the full file key as OpenObserve expects:
        // files/{org}/{stream_type}/{stream_name}/{YYYY}/{MM}/{DD}/{HH}/{filename}
        let file_key =
            format!("files/{org}/{stream_type}/{stream_name}/{date_key}/{registered_filename}");
        let (_, parsed_date, parsed_file) =
            parse_file_key_columns(&file_key).context("failed to parse file key")?;

        let result = conn.execute(
            "INSERT OR IGNORE INTO file_list
                (account, org, stream, date, file, deleted, flattened, min_ts, max_ts, records, original_size, compressed_size, index_size, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            rusqlite::params![
                account,
                org,
                &stream_key,
                &parsed_date,
                &parsed_file,
                false,
                false,
                meta.min_ts,
                meta.max_ts,
                meta.records,
                meta.original_size,
                meta.compressed_size,
                0i64, // index_size
                now_us,
            ],
        )?;

        if result > 0 {
            inserted += 1;
            println!(
                "  INSERT: {} (records={}, ts={}..{}, date={})",
                filename, meta.records, meta.min_ts, meta.max_ts, parsed_date
            );
        } else {
            skipped += 1;
            println!("  SKIP (duplicate): {}", filename);
        }
    }

    println!(
        "Done. Inserted: {}, Skipped: {}, Total: {}",
        inserted,
        skipped,
        files.len()
    );
    Ok(())
}

/// Parse file key into (stream_key, date_key, file_name), matching OpenObserve's format.
/// Key format: files/{org}/{stream_type}/{stream_name}/{YYYY}/{MM}/{DD}/{HH}/{filename}
fn parse_file_key_columns(key: &str) -> Result<(String, String, String)> {
    let columns: Vec<&str> = key.splitn(9, '/').collect();
    if columns.len() < 9 {
        bail!("invalid file path: {key}");
    }
    let stream_key = format!("{}/{}/{}", columns[1], columns[2], columns[3]);
    let date_key = format!(
        "{}/{}/{}/{}",
        columns[4], columns[5], columns[6], columns[7]
    );
    let file_name = columns[8].to_string();
    Ok((stream_key, date_key, file_name))
}
