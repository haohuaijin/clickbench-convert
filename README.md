# clickbench-convert

CLI tool for ClickBench dataset conversion. Supports adding a `_timestamp` column to parquet files and converting parquet to [Vortex](https://github.com/spiraldb/vortex) format.

## Build

```bash
cargo build --release
```

## Usage

### add-timestamp

Add an auto-incrementing `_timestamp` (i64) column to all parquet files in a directory. Files are processed in parallel; timestamps are globally ordered by sorted filename.

```bash
clichbench-convert add-timestamp --input clickbench/parquet --output clickbench/parquet_ts
```

### to-vortex

Convert parquet files to Vortex format.

```bash
clickbench-convert to-vortex --input clickbench/parquet_ts --output clickbench/vortex_ts
```

### register-file-list

Register parquet or vortex files into OpenObserve's `file_list` SQLite table. This allows OpenObserve to recognize and query externally produced data files.

```bash
# Register parquet files and copy them into OpenObserve's data directory
clickbench-convert register-file-list \
  --db /path/to/openobserve/db/metadata.sqlite \
  --input clickbench/parquet_ts \
  --data-dir /path/to/openobserve \
  --stream-type logs \
  --stream-name hits

# Register vortex files (metadata is read directly from the vortex footer)
clickbench-convert register-file-list \
  --db /path/to/openobserve/db/metadata.sqlite \
  --input clickbench/vortex_ts \
  --data-dir /path/to/openobserve \
  --stream-type logs \
  --stream-name hits
```

| Flag | Description | Default |
|------|-------------|---------|
| `--db` | Path to the SQLite database file | (required) |
| `-i, --input` | Directory containing `.parquet` or `.vortex` files | (required) |
| `--data-dir` | OpenObserve data directory. Files are copied to `{data-dir}/stream/files/...` | (optional) |
| `--stream-type` | Stream type (e.g., `logs`) | (required) |
| `--stream-name` | Stream name (e.g., `hits`) | (required) |
| `--org` | Organization ID | `default` |
| `--account` | Storage account name | `""` |
| `--parquet-metadata-dir` | Directory with corresponding parquet files for vortex metadata lookup | (optional) |

This command performs the following:
1. **Inserts file metadata** into the `file_list` table (idempotent — duplicates are skipped)
2. **Inserts the stream schema** into the `meta` table (Arrow schema as JSON, with `created_at`/`start_dt` metadata)
3. **Updates `stream_stats`** with aggregated file/record/timestamp counts
4. **Copies files** to the OpenObserve stream data directory (if `--data-dir` is provided): `{data-dir}/stream/files/{org}/{stream_type}/{stream_name}/{YYYY}/{MM}/{DD}/{HH}/{filename}`

For parquet files, metadata (`min_ts`, `max_ts`, `records`) is read from the parquet footer. For vortex files, metadata is read directly from the vortex file footer (row count, min/max timestamps from statistics, and Arrow schema from DType). All files use a fixed `original_size` of 2.1 GB.

## Typical workflow

```bash
# 1. Add _timestamp column to the raw ClickBench parquet files
cargo run --release -- add-timestamp -i clickbench/parquet -o clickbench/parquet_ts

# 2. Convert the timestamped parquet files to Vortex format
cargo run --release -- to-vortex -i clickbench/parquet_ts -o clickbench/vortex_ts

# 3. Register parquet files into OpenObserve (copies files + inserts metadata + schema)
cargo run --release -- register-file-list \
  --db ./data/openobserve/db/metadata.sqlite \
  --data-dir ./data/openobserve \
  -i clickbench/parquet_ts \
  --stream-type logs --stream-name hits

# Or register vortex files instead (reads metadata from vortex footer)
cargo run --release -- register-file-list \
  --db ./data/openobserve/db/metadata.sqlite \
  --data-dir ./data/openobserve \
  -i clickbench/vortex_ts \
  --stream-type logs --stream-name hits
```
