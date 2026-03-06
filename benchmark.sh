#!/bin/bash
set -e

# Configuration
OPENOBSERVE_VERSION=${OPENOBSERVE_VERSION:-"v0.60.0"}
OPENOBSERVE_DATA_DIR=${OPENOBSERVE_DATA_DIR:-"./data/openobserve"}
OPENOBSERVE_PORT=${OPENOBSERVE_PORT:-5080}
STREAM_NAME=${STREAM_NAME:-"hits"}
PARQUET_INPUT_DIR=${PARQUET_INPUT_DIR:-"clickbench/parquet"}
PARQUET_TS_DIR=${PARQUET_TS_DIR:-"clickbench/parquet_ts"}

# Create .env file for OpenObserve
cat > .env << 'ENVEOF'
ZO_ROOT_USER_EMAIL="root@example.com"
ZO_ROOT_USER_PASSWORD="Complexpass#123"
ZO_LOCAL_MODE=true
ZO_PRINT_KEY_SQL=true
ZO_UTF8_VIEW_ENABLED=false
ZO_RESULT_CACHE_ENABLED=false
ZO_FEATURE_PUSHDOWN_FILTER_ENABLED=false
ENVEOF

# Export variables from .env
set -a
source .env
set +a
export ZO_DATA_DIR="$OPENOBSERVE_DATA_DIR"

wait_for_openobserve() {
    echo "Waiting for OpenObserve to be ready..."
    for i in $(seq 1 30); do
        if curl -s "http://localhost:${OPENOBSERVE_PORT}/healthz" > /dev/null 2>&1; then
            echo "OpenObserve is ready."
            return 0
        fi
        sleep 2
    done
    echo "ERROR: OpenObserve did not become ready in time."
    exit 1
}

stop_openobserve() {
    if [ -n "$OPENOBSERVE_PID" ] && kill -0 "$OPENOBSERVE_PID" 2>/dev/null; then
        echo "Stopping OpenObserve (PID: $OPENOBSERVE_PID)..."
        kill "$OPENOBSERVE_PID"
        wait "$OPENOBSERVE_PID" 2>/dev/null || true
        echo "OpenObserve stopped."
    fi
}

trap stop_openobserve EXIT

# Step 0: Install build dependencies and Rust
if ! command -v g++ &> /dev/null; then
    echo "Install C++ compiler and build tools"
    sudo apt update && sudo apt install -y g++ build-essential
fi

if ! command -v cargo &> /dev/null; then
    echo "Install Rust"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > rust-init.sh
    bash rust-init.sh -y
    export HOME=${HOME:=~}
    source ~/.cargo/env
fi

echo "Build clickbench-convert"
cargo build --release
CONVERT_BIN="./target/release/clickbench-convert"

# Download OpenObserve binary if not present
OPENOBSERVE_BIN="./openobserve"
if [ ! -x "$OPENOBSERVE_BIN" ]; then
    echo "Download OpenObserve ${OPENOBSERVE_VERSION}"
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)
    case "$ARCH" in
        x86_64)  ARCH="amd64" ;;
        aarch64|arm64) ARCH="arm64" ;;
    esac
    TARBALL="openobserve-${OPENOBSERVE_VERSION}-${OS}-${ARCH}.tar.gz"
    wget -q "https://downloads.openobserve.ai/releases/openobserve/${OPENOBSERVE_VERSION}/${TARBALL}"
    tar xzf "$TARBALL"
    rm -f "$TARBALL"
    chmod +x "$OPENOBSERVE_BIN"
fi

# Step 0.5: Download ClickBench data if not present
if [ ! -d "$PARQUET_INPUT_DIR" ] || [ -z "$(ls -A "$PARQUET_INPUT_DIR" 2>/dev/null)" ]; then
    echo "Download benchmark target data, partitioned"
    mkdir -p "$PARQUET_INPUT_DIR"
    seq 0 99 | xargs -P100 -I{} bash -c "wget --directory-prefix $PARQUET_INPUT_DIR --continue --progress=dot:giga https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet"
fi

# Step 1: Start OpenObserve briefly to create the SQLite metadata tables
echo "=== Step 1: Start OpenObserve to initialize SQLite metadata ==="
$OPENOBSERVE_BIN &
OPENOBSERVE_PID=$!
wait_for_openobserve
stop_openobserve
unset OPENOBSERVE_PID
echo "SQLite metadata tables created."

# Step 2: Add _timestamp column to parquet files
echo "=== Step 2: Add _timestamp column to parquet files ==="
$CONVERT_BIN add-timestamp -i "$PARQUET_INPUT_DIR" -o "$PARQUET_TS_DIR"

# Step 3: Register files into OpenObserve (copies files + inserts metadata)
echo "=== Step 3: Register files into OpenObserve ==="
$CONVERT_BIN register-file-list \
    --db "${OPENOBSERVE_DATA_DIR}/db/metadata.sqlite" \
    --data-dir "$OPENOBSERVE_DATA_DIR" \
    -i "$PARQUET_TS_DIR" \
    --stream-name "$STREAM_NAME"

# Step 4: Start OpenObserve for benchmarking
echo "=== Step 4: Start OpenObserve ==="
$OPENOBSERVE_BIN &
OPENOBSERVE_PID=$!
wait_for_openobserve

# Step 5: Run the benchmark queries
echo "=== Step 5: Run benchmark queries ==="
./run.sh

stop_openobserve
unset OPENOBSERVE_PID

echo "Load time: 0"
echo "Data size: $(du -bcs "$PARQUET_TS_DIR" | grep total)"
