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

## Typical workflow

```bash
# 1. Add _timestamp column to the raw ClickBench parquet files
cargo run --release -- add-timestamp -i clickbench/parquet -o clickbench/parquet_ts

# 2. Convert the timestamped parquet files to Vortex format
cargo run --release -- to-vortex -i clickbench/parquet_ts -o clickbench/vortex
```
