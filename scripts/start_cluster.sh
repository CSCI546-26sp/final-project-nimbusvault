#!/usr/bin/env bash
# Starts a 3-node metadata cluster on localhost for development.
# Usage: ./scripts/start_cluster.sh [--mode baseline|adaptive] [--build-dir DIR]

set -e

MODE="adaptive"
BUILD_DIR="$(dirname "$0")/../build"
DATA_ROOT="/tmp/nimbusvault"

for arg in "$@"; do
  case $arg in
    --mode=*) MODE="${arg#*=}" ;;
    --build-dir=*) BUILD_DIR="${arg#*=}" ;;
  esac
done

META_BIN="$BUILD_DIR/meta_server"
if [[ ! -x "$META_BIN" ]]; then
  echo "ERROR: $META_BIN not found. Run cmake --build build first." >&2
  exit 1
fi

# Ports
LEADER_PORT=9100
F1_PORT=9101
F2_PORT=9102

# Data dirs
mkdir -p "$DATA_ROOT/meta0/wal" "$DATA_ROOT/meta1/wal" "$DATA_ROOT/meta2/wal"

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
  --peers "127.0.0.1:$LEADER_PORT" \
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
  --peers "127.0.0.1:$LEADER_PORT" \
  --data "$DATA_ROOT/meta2" \
  --mode "$MODE" \
  > "$DATA_ROOT/meta2/stdout.log" 2>&1 &
echo $! >> "$PID_FILE"

echo ""
echo "Cluster up (mode=$MODE):"
echo "  Leader   127.0.0.1:$LEADER_PORT  (PID $(sed -n '1p' $PID_FILE))"
echo "  Follower1 127.0.0.1:$F1_PORT    (PID $(sed -n '2p' $PID_FILE))"
echo "  Follower2 127.0.0.1:$F2_PORT    (PID $(sed -n '3p' $PID_FILE))"
echo ""
echo "Logs: $DATA_ROOT/meta{0,1,2}/stdout.log"
echo "Press Ctrl-C to stop."
echo ""

wait
