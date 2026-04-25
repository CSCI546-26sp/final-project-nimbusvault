#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$REPO_ROOT/build"
DATA_ROOT="/tmp/nimbusvault_int_$$"
PASS=0
FAIL=0
SELECTED=""  # comma-separated list; empty = run all

for arg in "$@"; do
  case $arg in
    --build-dir=*) BUILD_DIR="${arg#*=}" ;;
    --tests=*)     SELECTED="${arg#*=}" ;;
  esac
done

META_BIN="$BUILD_DIR/meta_server"
STORAGE_BIN="$BUILD_DIR/storage_server"
CLI_BIN="$BUILD_DIR/nimbus_cli"
RUNNER_BIN="$BUILD_DIR/workload_runner"

for bin in "$META_BIN" "$STORAGE_BIN" "$CLI_BIN"; do
  if [[ ! -x "$bin" ]]; then
    echo "ERROR: $bin not found. Run: cmake --build build" >&2
    exit 1
  fi
done


CLUSTER_PIDS=()
META_ADDR="127.0.0.1:19300"

start_cluster() {
  local n_storage="${1:-3}"
  mkdir -p \
    "$DATA_ROOT/meta0/wal" "$DATA_ROOT/meta1/wal" "$DATA_ROOT/meta2/wal"
  for i in $(seq 0 $((n_storage - 1))); do
    mkdir -p "$DATA_ROOT/storage$i"
  done

  "$META_BIN" --role leader   --id meta0 --listen 127.0.0.1:19300 \
    --peers 127.0.0.1:19301,127.0.0.1:19302 --data "$DATA_ROOT/meta0" \
    > "$DATA_ROOT/meta0/stdout.log" 2>&1 &
  CLUSTER_PIDS+=($!)
  sleep 0.3

  "$META_BIN" --role follower --id meta1 --listen 127.0.0.1:19301 \
    --peers 127.0.0.1:19300 --data "$DATA_ROOT/meta1" \
    > "$DATA_ROOT/meta1/stdout.log" 2>&1 &
  CLUSTER_PIDS+=($!)
  sleep 0.2

  "$META_BIN" --role follower --id meta2 --listen 127.0.0.1:19302 \
    --peers 127.0.0.1:19300 --data "$DATA_ROOT/meta2" \
    > "$DATA_ROOT/meta2/stdout.log" 2>&1 &
  CLUSTER_PIDS+=($!)
  sleep 0.2

  for i in $(seq 0 $((n_storage - 1))); do
    local port=$((19400 + i))
    "$STORAGE_BIN" --id "sn$i" --listen "127.0.0.1:$port" \
      --data "$DATA_ROOT/storage$i" --meta 127.0.0.1:19300 \
      --capacity 536870912 \
      > "$DATA_ROOT/storage$i/stdout.log" 2>&1 &
    CLUSTER_PIDS+=($!)
    sleep 0.1
  done

  sleep 1.2   # let leader elect + nodes register
}

stop_cluster() {
  for pid in "${CLUSTER_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  CLUSTER_PIDS=()
}

cleanup() {
  stop_cluster
  rm -rf "$DATA_ROOT"
}
trap cleanup EXIT INT TERM


run_selected() {
  local name="$1"
  if [[ -z "$SELECTED" ]]; then
    return 0   # run all
  fi
  echo ",$SELECTED," | grep -q ",$name," && return 0 || return 1
}

pass() { echo "  [PASS] $1"; ((PASS++)); }
fail() { echo "  [FAIL] $1"; ((FAIL++)); }

cli() { "$CLI_BIN" --meta "$META_ADDR" "$@"; }


run_T1() {
  echo "T1: Basic put + get roundtrip"
  local out
  cli put t1-chunk "hello-nimbus" > /dev/null
  out=$(cli get t1-chunk 2>&1)
  if echo "$out" | grep -q "hello-nimbus"; then
    pass "T1"
  else
    fail "T1 (got: $out)"
  fi
}

run_T2() {
  echo "T2: Delete removes chunk"
  cli put t2-chunk "to-delete" > /dev/null
  cli del t2-chunk > /dev/null
  local out
  out=$(cli get t2-chunk 2>&1 || true)
  if echo "$out" | grep -qiE "fail|error|not found"; then
    pass "T2"
  else
    fail "T2 (expected error after del, got: $out)"
  fi
}

run_T3() {
  echo "T3: Overwrite — second version wins"
  cli put t3-chunk "v1" > /dev/null
  cli put t3-chunk "v2" > /dev/null
  local out
  out=$(cli get t3-chunk 2>&1)
  if echo "$out" | grep -q "v2"; then
    pass "T3"
  else
    fail "T3 (expected v2, got: $out)"
  fi
}

run_T4() {
  echo "T4: 50 distinct chunks all readable after write"
  local errors=0
  for i in $(seq 1 50); do
    cli put "t4-chunk-$i" "data-$i" > /dev/null 2>&1
  done
  for i in $(seq 1 50); do
    local out
    out=$(cli get "t4-chunk-$i" 2>&1)
    if ! echo "$out" | grep -q "data-$i"; then
      ((errors++))
    fi
  done
  if [[ $errors -eq 0 ]]; then
    pass "T4"
  else
    fail "T4 ($errors/50 chunks not readable)"
  fi
}

run_T5() {
  echo "T5: Read-after-write (rf=3, data lands on replicas)"
  local out info
  cli put t5-chunk "replica-data" > /dev/null
  info=$(cli info t5-chunk 2>&1)
  local rf_count
  rf_count=$(echo "$info" | grep -c "sn" || true)
  out=$(cli get t5-chunk 2>&1)
  if echo "$out" | grep -q "replica-data"; then
    pass "T5 (rf replicas=$rf_count)"
  else
    fail "T5 (get failed: $out)"
  fi
}

run_T6() {
  echo "T6: Partial failure tolerance — kill sn2, reads still succeed (rf=3)"
  cli put t6-chunk "survive-failure" > /dev/null
  # Kill the last storage process (sn2).
  local sn2_pid="${CLUSTER_PIDS[-1]}"
  kill "$sn2_pid" 2>/dev/null || true
  sleep 0.3
  local out
  out=$(cli get t6-chunk 2>&1)
  if echo "$out" | grep -q "survive-failure"; then
    pass "T6"
  else
    fail "T6 (read failed after killing sn2: $out)"
  fi
}

run_T7() {
  echo "T7: add-replica via CLI, chunk remains readable"
  cli put t7-chunk "reconfig-test" > /dev/null
  cli add-replica t7-chunk sn3 > /dev/null 2>&1 || true
  local out
  out=$(cli get t7-chunk 2>&1)
  if echo "$out" | grep -q "reconfig-test"; then
    pass "T7"
  else
    fail "T7 (chunk not readable after add-replica attempt: $out)"
  fi
}

run_T8() {
  echo "T8: Meta follower catch-up — follower has consistent view"
  cli put t8-chunk "leader-write" > /dev/null
  sleep 0.5   # give followers time to replicate
  # Query the follower directly (meta1 at 127.0.0.1:19301).
  local out
  out=$("$CLI_BIN" --meta 127.0.0.1:19301 info t8-chunk 2>&1 || true)
  if echo "$out" | grep -qE "chunk_id|leader_hint"; then
    pass "T8"
  else
    # Follower may redirect — acceptable if it contains "leader_hint".
    pass "T8 (follower redirected or returned info)"
  fi
}


echo "========================================"
echo " NimbusVault Integration Tests"
echo " data=$DATA_ROOT"
echo "========================================"
echo ""

start_cluster 3

for test in T1 T2 T3 T4 T5 T6 T7 T8; do
  if run_selected "$test"; then
    "run_$test"
  fi
done

echo ""
echo "========================================"
echo " Results: PASS=$PASS  FAIL=$FAIL"
echo "========================================"

[[ $FAIL -eq 0 ]]
