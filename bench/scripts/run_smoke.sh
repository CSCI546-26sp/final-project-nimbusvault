#!/usr/bin/env bash
# Smoke test: spin up a 3-meta + 3-storage cluster, run 1 000 ops, expect 0 errors.
# Exit code 0 = pass, non-zero = fail.
#
# Usage:
#   ./bench/scripts/run_smoke.sh [--build-dir DIR]
#
# Prerequisites: build/ must contain meta_server, storage_server, workload_runner.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$REPO_ROOT/build"
DATA_ROOT="/tmp/nimbusvault_smoke_$$"
CSV="$DATA_ROOT/smoke.csv"

for arg in "$@"; do
  case $arg in
    --build-dir=*) BUILD_DIR="${arg#*=}" ;;
  esac
done

META_BIN="$BUILD_DIR/meta_server"
STORAGE_BIN="$BUILD_DIR/storage_server"
RUNNER_BIN="$BUILD_DIR/workload_runner"

for bin in "$META_BIN" "$STORAGE_BIN" "$RUNNER_BIN"; do
  if [[ ! -x "$bin" ]]; then
    echo "ERROR: $bin not found. Run: cmake --build build" >&2
    exit 1
  fi
done

mkdir -p \
  "$DATA_ROOT/meta0/wal" "$DATA_ROOT/meta1/wal" "$DATA_ROOT/meta2/wal" \
  "$DATA_ROOT/storage0"  "$DATA_ROOT/storage1"  "$DATA_ROOT/storage2"

PIDS=()

cleanup() {
  echo "[smoke] Stopping cluster..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$DATA_ROOT"
}
trap cleanup EXIT INT TERM

echo "[smoke] Starting 3-meta cluster..."
"$META_BIN" --role leader   --id meta0 --listen 127.0.0.1:19100 \
  --peers 127.0.0.1:19101,127.0.0.1:19102 --data "$DATA_ROOT/meta0" \
  > "$DATA_ROOT/meta0/stdout.log" 2>&1 &
PIDS+=($!)
sleep 0.3

"$META_BIN" --role follower --id meta1 --listen 127.0.0.1:19101 \
  --peers 127.0.0.1:19100 --data "$DATA_ROOT/meta1" \
  > "$DATA_ROOT/meta1/stdout.log" 2>&1 &
PIDS+=($!)
sleep 0.2

"$META_BIN" --role follower --id meta2 --listen 127.0.0.1:19102 \
  --peers 127.0.0.1:19100 --data "$DATA_ROOT/meta2" \
  > "$DATA_ROOT/meta2/stdout.log" 2>&1 &
PIDS+=($!)
sleep 0.2

echo "[smoke] Starting 3 storage nodes..."
for i in 0 1 2; do
  PORT=$((19200 + i))
  "$STORAGE_BIN" --id "sn$i" --listen "127.0.0.1:$PORT" \
    --data "$DATA_ROOT/storage$i" --meta 127.0.0.1:19100 \
    --capacity 1073741824 \
    > "$DATA_ROOT/storage$i/stdout.log" 2>&1 &
  PIDS+=($!)
  sleep 0.1
done

# Give the cluster time to elect a leader and register nodes.
sleep 1.0

echo "[smoke] Running 1 000 ops (mode=adaptive, rf=3)..."
"$RUNNER_BIN" \
  --meta 127.0.0.1:19100 \
  --mode adaptive \
  --chunks 20 \
  --ops 1000 \
  --chunk-size 4 \
  --write-ratio 0.3 \
  --threads 2 \
  --desired-rf 3 \
  --output "$CSV"

# Parse error count from the CSV (column 5, ok=0 rows, skipping header).
ERRORS=$(tail -n +2 "$CSV" | awk -F',' '$5 == 0' | wc -l | tr -d ' ')

echo ""
echo "[smoke] ──────────────────────────────"
echo "[smoke] Total CSV rows : $(tail -n +2 "$CSV" | wc -l | tr -d ' ')"
echo "[smoke] Error rows     : $ERRORS"

if [[ "$ERRORS" -eq 0 ]]; then
  echo "[smoke] PASS — 0 errors"
  exit 0
else
  echo "[smoke] FAIL — $ERRORS errors detected"
  echo "[smoke] First few error rows:"
  tail -n +2 "$CSV" | awk -F',' '$5 == 0' | head -5
  exit 1
fi
