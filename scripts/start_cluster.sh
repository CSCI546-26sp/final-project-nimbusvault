#!/usr/bin/env bash
# Starts a 3-node metadata cluster + 3 storage nodes on localhost for development.
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
STORAGE_BIN="$BUILD_DIR/storage_server"
if [[ ! -x "$META_BIN" ]]; then
  echo "ERROR: $META_BIN not found. Run cmake --build build first." >&2
  exit 1
fi
if [[ ! -x "$STORAGE_BIN" ]]; then
  echo "ERROR: $STORAGE_BIN not found. Run cmake --build build first." >&2
  exit 1
fi

# Ports
LEADER_PORT=9100
F1_PORT=9101
F2_PORT=9102
S0_PORT=9200
S1_PORT=9201
S2_PORT=9202

# Data dirs
mkdir -p "$DATA_ROOT/meta0/wal" "$DATA_ROOT/meta1/wal" "$DATA_ROOT/meta2/wal"
mkdir -p "$DATA_ROOT/storage0" "$DATA_ROOT/storage1" "$DATA_ROOT/storage2"

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

echo "Starting storage node sn0 on :$S0_PORT ..."
"$STORAGE_BIN" \
  --id sn0 \
  --listen "127.0.0.1:$S0_PORT" \
  --data "$DATA_ROOT/storage0" \
  --meta "127.0.0.1:$LEADER_PORT" \
  --capacity 10737418240 \
  > "$DATA_ROOT/storage0/stdout.log" 2>&1 &
echo $! >> "$PID_FILE"
sleep 0.2

echo "Starting storage node sn1 on :$S1_PORT ..."
"$STORAGE_BIN" \
  --id sn1 \
  --listen "127.0.0.1:$S1_PORT" \
  --data "$DATA_ROOT/storage1" \
  --meta "127.0.0.1:$LEADER_PORT" \
  --capacity 10737418240 \
  > "$DATA_ROOT/storage1/stdout.log" 2>&1 &
echo $! >> "$PID_FILE"
sleep 0.2

echo "Starting storage node sn2 on :$S2_PORT ..."
"$STORAGE_BIN" \
  --id sn2 \
  --listen "127.0.0.1:$S2_PORT" \
  --data "$DATA_ROOT/storage2" \
  --meta "127.0.0.1:$LEADER_PORT" \
  --capacity 10737418240 \
  > "$DATA_ROOT/storage2/stdout.log" 2>&1 &
echo $! >> "$PID_FILE"

echo ""
echo "Cluster up (mode=$MODE):"
echo "  Leader   127.0.0.1:$LEADER_PORT  (PID $(sed -n '1p' $PID_FILE))"
echo "  Follower1 127.0.0.1:$F1_PORT    (PID $(sed -n '2p' $PID_FILE))"
echo "  Follower2 127.0.0.1:$F2_PORT    (PID $(sed -n '3p' $PID_FILE))"
echo "  Storage0 127.0.0.1:$S0_PORT    (PID $(sed -n '4p' $PID_FILE))"
echo "  Storage1 127.0.0.1:$S1_PORT    (PID $(sed -n '5p' $PID_FILE))"
echo "  Storage2 127.0.0.1:$S2_PORT    (PID $(sed -n '6p' $PID_FILE))"
echo ""
echo "Meta logs:    $DATA_ROOT/meta{0,1,2}/stdout.log"
echo "Storage logs: $DATA_ROOT/storage{0,1,2}/stdout.log"
echo "Press Ctrl-C to stop."
echo ""

wait
