#!/usr/bin/env bash

# run_benchmark.sh
# Automates starting the cluster, running the workload comparison, and stopping the cluster.

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$DIR/../.."
BUILD_DIR="$ROOT_DIR/build"

# Configuration variables
OPS=${1:-20000}
CHUNKS=${2:-1000}
THREADS=${3:-4}
WRITE_RATIO=${4:-0.2}

echo "=========================================="
echo " Starting NimbusVault Benchmark Run       "
echo " Ops: $OPS | Chunks: $CHUNKS | Threads: $THREADS "
echo "=========================================="

# 1. Start the cluster in the background
echo "[1/3] Starting local cluster..."
bash "$ROOT_DIR/scripts/start_cluster.sh" &
CLUSTER_PID=$!

# Wait for cluster to initialize
echo "Waiting 5 seconds for cluster to initialize..."
sleep 5

# 2. Run the workload in compare mode
echo "[2/3] Running workload comparison..."
if [[ ! -x "$BUILD_DIR/workload_runner" ]]; then
    echo "ERROR: workload_runner not found in $BUILD_DIR"
    kill $CLUSTER_PID
    exit 1
fi

"$BUILD_DIR/workload_runner" \
    --meta 127.0.0.1:9100 \
    --mode compare \
    --ops "$OPS" \
    --chunks "$CHUNKS" \
    --threads "$THREADS" \
    --write-ratio "$WRITE_RATIO" \
    --output "$ROOT_DIR/benchmark_results.csv"

# 3. Cleanup
echo "[3/3] Shutting down cluster..."
kill -SIGTERM $CLUSTER_PID
wait $CLUSTER_PID 2>/dev/null || true

echo "=========================================="
echo " Benchmark Complete. "
echo " Results saved to benchmark_results.csv "
echo "=========================================="