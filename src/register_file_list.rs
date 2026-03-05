use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use arrow::datatypes::Schema;
use chrono::TimeZone;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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

/// Read the Arrow schema from a parquet file.
fn read_parquet_arrow_schema(path: &Path) -> Result<Schema> {
    let file =
        std::fs::File::open(path).with_context(|| format!("failed to open: {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(reader.schema().as_ref().clone())
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

/// Copy a file to the OpenObserve stream data directory.
/// Returns the destination path.
fn copy_to_data_dir(
    src: &Path,
    data_dir: &Path,
    org: &str,
    stream_type: &str,
    stream_name: &str,
    date_key: &str,
    filename: &str,
) -> Result<PathBuf> {
    // OpenObserve stores files at: {data_dir}/stream/files/{org}/{stream_type}/{stream_name}/{YYYY}/{MM}/{DD}/{HH}/{filename}
    let dest_dir = data_dir
        .join("stream")
        .join("files")
        .join(org)
        .join(stream_type)
        .join(stream_name)
        .join(date_key);
    std::fs::create_dir_all(&dest_dir)
        .with_context(|| format!("failed to create directory: {}", dest_dir.display()))?;
    let dest = dest_dir.join(filename);
    if !dest.exists() {
        std::fs::copy(src, &dest).with_context(|| {
            format!(
                "failed to copy {} -> {}",
                src.display(),
                dest.display()
            )
        })?;
    }
    Ok(dest)
}

/// Insert the stream schema into the meta table (OpenObserve format).
/// Schema is stored as JSON-serialized Vec<Schema> with metadata containing created_at and start_dt.
fn insert_schema(
    conn: &Connection,
    org: &str,
    stream_type: &str,
    stream_name: &str,
    schema: &Schema,
    start_dt: i64,
) -> Result<()> {
    // Check if schema already exists
    let key2 = format!("{stream_type}/{stream_name}");
    let exists: bool = conn.query_row(
        "SELECT COUNT(*) > 0 FROM meta WHERE module = 'schema' AND key1 = ?1 AND key2 = ?2",
        rusqlite::params![org, &key2],
        |row| row.get(0),
    )?;

    if exists {
        println!("  Schema already exists for {org}/{stream_type}/{stream_name}, skipping");
        return Ok(());
    }

    // Add OpenObserve-required metadata to the schema
    let mut metadata = schema.metadata().clone();
    if !metadata.contains_key("created_at") {
        metadata.insert("created_at".to_string(), start_dt.to_string());
    }
    if !metadata.contains_key("start_dt") {
        metadata.insert("start_dt".to_string(), start_dt.to_string());
    }
    let schema_with_meta = schema.clone().with_metadata(metadata);

    // OpenObserve stores schemas as JSON-serialized Vec<Schema>
    let schemas = vec![schema_with_meta];
    let value = serde_json::to_string(&schemas)?;

    conn.execute(
        "INSERT INTO meta (module, key1, key2, start_dt, value) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params!["schema", org, &key2, start_dt, &value],
    )?;

    println!(
        "  Schema inserted for {org}/{stream_type}/{stream_name} ({} fields)",
        schema.fields().len()
    );
    Ok(())
}

/// Insert or update stream_stats for the stream.
fn upsert_stream_stats(
    conn: &Connection,
    org: &str,
    stream_type: &str,
    stream_name: &str,
    file_count: i64,
    min_ts: i64,
    max_ts: i64,
    total_records: i64,
    total_original_size: i64,
    total_compressed_size: i64,
) -> Result<()> {
    let stream_key = format!("{org}/{stream_type}/{stream_name}");

    // Check if stream_stats table exists and has data
    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM stream_stats WHERE stream = ?1 AND is_recent = FALSE",
            rusqlite::params![&stream_key],
            |row| row.get(0),
        )
        .unwrap_or(false);

    if exists {
        conn.execute(
            "UPDATE stream_stats SET file_num = ?1, min_ts = ?2, max_ts = ?3, records = ?4, original_size = ?5, compressed_size = ?6
             WHERE stream = ?7 AND is_recent = FALSE",
            rusqlite::params![
                file_count,
                min_ts,
                max_ts,
                total_records,
                total_original_size,
                total_compressed_size,
                &stream_key,
            ],
        )?;
    } else {
        conn.execute(
            "INSERT INTO stream_stats (org, stream, file_num, min_ts, max_ts, records, original_size, compressed_size, index_size, is_recent)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, FALSE)",
            rusqlite::params![
                org,
                &stream_key,
                file_count,
                min_ts,
                max_ts,
                total_records,
                total_original_size,
                total_compressed_size,
            ],
        )?;
    }

    println!(
        "  Stream stats updated: files={file_count}, records={total_records}, ts={min_ts}..{max_ts}"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn register_file_list(
    db_path: &Path,
    input_dir: &Path,
    data_dir: Option<&Path>,
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

    // Detect columns in the existing table
    let has_created_at = {
        let mut stmt = conn.prepare("PRAGMA table_info(file_list)")?;
        let cols: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .filter_map(|r| r.ok())
            .collect();
        cols.iter().any(|c| c == "created_at")
    };

    let stream_key = format!("{org}/{stream_type}/{stream_name}");
    let now_us = chrono::Utc::now().timestamp_micros();

    let mut inserted = 0;
    let mut skipped = 0;
    let mut schema_inserted = false;
    let mut global_min_ts = i64::MAX;
    let mut global_max_ts = i64::MIN;
    let mut total_records: i64 = 0;
    let mut total_original_size: i64 = 0;
    let mut total_compressed_size: i64 = 0;

    for path in &files {
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .context("invalid filename")?;

        // Resolve the parquet file for metadata (and schema)
        let (meta, registered_filename, parquet_path_for_schema) = match ext {
            "parquet" => {
                let meta = read_parquet_metadata(path)
                    .with_context(|| format!("failed to read metadata: {}", path.display()))?;
                (meta, filename.to_string(), path.to_path_buf())
            }
            "vortex" => {
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
                meta.compressed_size = std::fs::metadata(path)?.len() as i64;
                (meta, filename.to_string(), parquet_path)
            }
            _ => continue,
        };

        // Insert schema from the first file (all files in a stream share the same schema)
        if !schema_inserted {
            let arrow_schema = read_parquet_arrow_schema(&parquet_path_for_schema)
                .with_context(|| "failed to read arrow schema")?;
            insert_schema(&conn, org, stream_type, stream_name, &arrow_schema, meta.min_ts)?;
            schema_inserted = true;
        }

        let date_key = date_key_from_ts(meta.min_ts);

        // Copy file to data directory if --data-dir is specified
        if let Some(data_dir) = data_dir {
            copy_to_data_dir(
                path,
                data_dir,
                org,
                stream_type,
                stream_name,
                &date_key,
                &registered_filename,
            )?;
        }

        // Construct the full file key as OpenObserve expects
        let file_key =
            format!("files/{org}/{stream_type}/{stream_name}/{date_key}/{registered_filename}");
        let (_, parsed_date, parsed_file) =
            parse_file_key_columns(&file_key).context("failed to parse file key")?;

        let result = if has_created_at {
            conn.execute(
                "INSERT OR IGNORE INTO file_list
                    (account, org, stream, date, file, deleted, flattened, min_ts, max_ts, records, original_size, compressed_size, index_size, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
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
                    0i64,
                    now_us,
                    now_us,
                ],
            )?
        } else {
            conn.execute(
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
                    0i64,
                    now_us,
                ],
            )?
        };

        if result > 0 {
            inserted += 1;
            global_min_ts = global_min_ts.min(meta.min_ts);
            global_max_ts = global_max_ts.max(meta.max_ts);
            total_records += meta.records;
            total_original_size += meta.original_size;
            total_compressed_size += meta.compressed_size;
            println!(
                "  INSERT: {} (records={}, ts={}..{}, date={})",
                filename, meta.records, meta.min_ts, meta.max_ts, parsed_date
            );
        } else {
            skipped += 1;
            println!("  SKIP (duplicate): {}", filename);
        }
    }

    // Update stream_stats
    if inserted > 0 {
        upsert_stream_stats(
            &conn,
            org,
            stream_type,
            stream_name,
            inserted,
            global_min_ts,
            global_max_ts,
            total_records,
            total_original_size,
            total_compressed_size,
        )?;
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
