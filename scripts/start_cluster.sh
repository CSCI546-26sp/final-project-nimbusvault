#!/usr/bin/env bash
# Starts a 3-node metadata cluster + N storage nodes on localhost for development.
#
# Usage:
#   ./scripts/start_cluster.sh [--mode baseline|adaptive] \
#                               [--storage-nodes N] \
#                               [--build-dir DIR]
#
# --storage-nodes N  : number of storage nodes to start (default 3; supports 4, 8, 16, ...)
# --mode             : workload mode hint written to logs (default adaptive)
# --build-dir        : directory containing compiled binaries (default ../build)

set -e

MODE="adaptive"
BUILD_DIR="$(dirname "$0")/../build"
DATA_ROOT="/tmp/nimbusvault"
N_STORAGE=3

for arg in "$@"; do
  case $arg in
    --mode=*)          MODE="${arg#*=}" ;;
    --build-dir=*)     BUILD_DIR="${arg#*=}" ;;
    --storage-nodes=*) N_STORAGE="${arg#*=}" ;;
  esac
done

META_BIN="$BUILD_DIR/meta_server"
STORAGE_BIN="$BUILD_DIR/storage_server"
if [[ ! -x "$META_BIN" ]]; then
  echo "ERROR: $META_BIN not found. Run cmake --build build first." >&2
  exit 1
fi
if [[ ! -x "$STORAGE_BIN" ]]; then
  echo "ERROR: $STORAGE_BIN not found. Run cmake --build build first." >&2
  exit 1
fi

# Meta ports (fixed 3-node meta cluster).
LEADER_PORT=9100
F1_PORT=9101
F2_PORT=9102

# Storage base port: sn0 → 9200, sn1 → 9201, ...
STORAGE_BASE_PORT=9200

# Data dirs.
mkdir -p "$DATA_ROOT/meta0/wal" "$DATA_ROOT/meta1/wal" "$DATA_ROOT/meta2/wal"
for i in $(seq 0 $((N_STORAGE - 1))); do
  mkdir -p "$DATA_ROOT/storage$i"
done

PID_FILE="$DATA_ROOT/cluster.pids"
> "$PID_FILE"

cleanup() {
  echo "Stopping cluster..."
  if [[ -f "$PID_FILE" ]]; then
    while read -r pid; do
      kill "$pid" 2>/dev/null || true
    done < "$PID_FILE"
  fi
  echo "Done."
}
trap cleanup EXIT INT TERM

echo "Starting leader (meta0) on :$LEADER_PORT ..."
"$META_BIN" \
  --role leader \
  --id meta0 \
  --listen "127.0.0.1:$LEADER_PORT" \
  --peers "127.0.0.1:$F1_PORT,127.0.0.1:$F2_PORT" \
  --data "$DATA_ROOT/meta0" \
  --mode "$MODE" \
  > "$DATA_ROOT/meta0/stdout.log" 2>&1 &
echo $! >> "$PID_FILE"
sleep 0.3

echo "Starting follower 1 (meta1) on :$F1_PORT ..."
"$META_BIN" \
  --role follower \
  --id meta1 \
  --listen "127.0.0.1:$F1_PORT" \
  --peers "127.0.0.1:$LEADER_PORT,127.0.0.1:$F2_PORT" \
  --data "$DATA_ROOT/meta1" \
  --mode "$MODE" \
  > "$DATA_ROOT/meta1/stdout.log" 2>&1 &
echo $! >> "$PID_FILE"
sleep 0.3

echo "Starting follower 2 (meta2) on :$F2_PORT ..."
"$META_BIN" \
  --role follower \
  --id meta2 \
  --listen "127.0.0.1:$F2_PORT" \
  --peers "127.0.0.1:$LEADER_PORT,127.0.0.1:$F1_PORT" \
  --data "$DATA_ROOT/meta2" \
  --mode "$MODE" \
  > "$DATA_ROOT/meta2/stdout.log" 2>&1 &
echo $! >> "$PID_FILE"

echo "Starting $N_STORAGE storage nodes (base port $STORAGE_BASE_PORT)..."
for i in $(seq 0 $((N_STORAGE - 1))); do
  PORT=$((STORAGE_BASE_PORT + i))
  echo "  sn$i  127.0.0.1:$PORT"
  "$STORAGE_BIN" \
    --id "sn$i" \
    --listen "127.0.0.1:$PORT" \
    --data "$DATA_ROOT/storage$i" \
    --meta "127.0.0.1:$LEADER_PORT,127.0.0.1:$F1_PORT,127.0.0.1:$F2_PORT" \
    --capacity 10737418240 \
    > "$DATA_ROOT/storage$i/stdout.log" 2>&1 &
  echo $! >> "$PID_FILE"
  sleep 0.1
done

echo ""
echo "Cluster up (mode=$MODE, storage_nodes=$N_STORAGE):"
echo "  Leader    127.0.0.1:$LEADER_PORT"
echo "  Follower1 127.0.0.1:$F1_PORT"
echo "  Follower2 127.0.0.1:$F2_PORT"
for i in $(seq 0 $((N_STORAGE - 1))); do
  PORT=$((STORAGE_BASE_PORT + i))
  echo "  sn$i      127.0.0.1:$PORT"
done
echo ""
echo "Meta logs:    $DATA_ROOT/meta{0,1,2}/stdout.log"
echo "Storage logs: $DATA_ROOT/storage{0...$((N_STORAGE-1))}/stdout.log"
echo "Press Ctrl-C to stop."
echo ""

wait
