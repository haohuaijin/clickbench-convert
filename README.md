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
# Register parquet files
clickbench-convert register-file-list \
  --db /path/to/file_list.sqlite \
  --input clickbench/parquet_ts \
  --stream-type logs \
  --stream-name hits

# Register vortex files (reads metadata from corresponding parquet files)
clickbench-convert register-file-list \
  --db /path/to/file_list.sqlite \
  --input clickbench/vortex_ts \
  --stream-type logs \
  --stream-name hits \
  --parquet-metadata-dir clickbench/parquet_ts
```

| Flag | Description | Default |
|------|-------------|---------|
| `--db` | Path to the SQLite database file | (required) |
| `-i, --input` | Directory containing `.parquet` or `.vortex` files | (required) |
| `--stream-type` | Stream type (e.g., `logs`) | (required) |
| `--stream-name` | Stream name (e.g., `hits`) | (required) |
| `--org` | Organization ID | `default` |
| `--account` | Storage account name | `""` |
| `--parquet-metadata-dir` | Directory with corresponding parquet files for vortex metadata lookup | (optional) |

For parquet files, metadata (`min_ts`, `max_ts`, `records`, `original_size`) is read from the parquet footer. For vortex files, metadata is read from the corresponding parquet file (same basename, `.parquet` extension) found in `--parquet-metadata-dir` (or `--input` if not specified).

## Typical workflow

```bash
# 1. Add _timestamp column to the raw ClickBench parquet files
cargo run --release -- add-timestamp -i clickbench/parquet -o clickbench/parquet_ts

# 2. Convert the timestamped parquet files to Vortex format
cargo run --release -- to-vortex -i clickbench/parquet_ts -o clickbench/vortex_ts

# 3. Register parquet files into OpenObserve's file_list
cargo run --release -- register-file-list \
  --db /path/to/openobserve/file_list.sqlite \
  -i clickbench/parquet_ts \
  --stream-type logs --stream-name hits

# Or register vortex files instead
cargo run --release -- register-file-list \
  --db /path/to/openobserve/file_list.sqlite \
  -i clickbench/vortex_ts \
  --stream-type logs --stream-name hits \
  --parquet-metadata-dir clickbench/parquet_ts
```
