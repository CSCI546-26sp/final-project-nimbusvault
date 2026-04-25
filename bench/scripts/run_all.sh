#!/usr/bin/env bash
# Full benchmark matrix driver.
#
# Sweeps:
#   modes        : baseline, adaptive
#   distributions: hot_cold, zipfian, uniform
#   chunk sizes  : 4 KB, 64 KB, 256 KB
#   desired_rf   : 2, 3, 5 (adaptive mode only)
#
# Each combination writes a CSV to bench/results/ and appends a row to
# bench/results/summary.tsv for post-hoc plotting.
#
# Usage:
#   ./bench/scripts/run_all.sh [--build-dir DIR] [--ops N] [--chunks N]
#
# Prerequisites: a running cluster reachable at META_ADDR (default 127.0.0.1:9100).
# Start one first with: ./scripts/start_cluster.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$REPO_ROOT/build"
RESULTS_DIR="$REPO_ROOT/bench/results"
META_ADDR="127.0.0.1:9100"
OPS=5000
CHUNKS=50
THREADS=4

for arg in "$@"; do
  case $arg in
    --build-dir=*) BUILD_DIR="${arg#*=}" ;;
    --meta=*)      META_ADDR="${arg#*=}" ;;
    --ops=*)       OPS="${arg#*=}" ;;
    --chunks=*)    CHUNKS="${arg#*=}" ;;
    --threads=*)   THREADS="${arg#*=}" ;;
  esac
done

RUNNER="$BUILD_DIR/workload_runner"
if [[ ! -x "$RUNNER" ]]; then
  echo "ERROR: $RUNNER not found. Run: cmake --build build" >&2
  exit 1
fi

mkdir -p "$RESULTS_DIR"
SUMMARY="$RESULTS_DIR/summary.tsv"

# Write header if fresh file.
if [[ ! -f "$SUMMARY" ]]; then
  printf "mode\tdistribution\tchunk_kb\tdesired_rf\tops\terrors\tthroughput_ops_s\tp50_us\tp95_us\tp99_us\tput_mb_s\tget_mb_s\tcsv\n" \
    > "$SUMMARY"
fi

run_one() {
  local mode="$1" dist="$2" chunk_kb="$3" rf="$4"
  local label="${mode}_${dist}_${chunk_kb}kb_rf${rf}"
  local csv="$RESULTS_DIR/${label}.csv"

  echo "── $label ──"
  "$RUNNER" \
    --meta "$META_ADDR" \
    --mode "$mode" \
    --distribution "$dist" \
    --chunk-size "$chunk_kb" \
    --chunks "$CHUNKS" \
    --ops "$OPS" \
    --threads "$THREADS" \
    --desired-rf "$rf" \
    --output "$csv"

  # Extract stats from CSV.
  local total errors p50 p95 p99
  total=$(tail -n +2 "$csv" | grep -v TIER_CHANGE | wc -l | tr -d ' ')
  errors=$(tail -n +2 "$csv" | awk -F',' '$5 == 0 && $2 != "TIER_CHANGE"' | wc -l | tr -d ' ')

  # Latency percentiles (column 4) from non-TIER_CHANGE rows, sorted.
  local latencies
  latencies=$(tail -n +2 "$csv" | awk -F',' '$2 != "TIER_CHANGE" {print $4}' | sort -n)
  p50=$(echo "$latencies" | awk "NR==int($total*0.50+0.5){print}")
  p95=$(echo "$latencies" | awk "NR==int($total*0.95+0.5){print}")
  p99=$(echo "$latencies" | awk "NR==int($total*0.99+0.5){print}")
  p50=${p50:-0}; p95=${p95:-0}; p99=${p99:-0}

  # Time window for throughput: max(end_us) - min(start_us) in seconds.
  local dur_s tput
  dur_s=$(tail -n +2 "$csv" | awk -F',' '$2 != "TIER_CHANGE" {
      s=$1; e=$1+$4
      if (min_s=="" || s<min_s) min_s=s
      if (e>max_e) max_e=e
    } END {
      dur=(max_e-min_s)/1e6; if (dur<1) dur=1; print dur
    }')
  dur_s=${dur_s:-1}
  tput=$(awk "BEGIN{printf \"%.1f\", $total / $dur_s}")

  # PUT/GET MB/s.
  local put_mb get_mb
  put_mb=$(tail -n +2 "$csv" | awk -F',' '$2=="PUT" && $5==1 {s+=$6} END {printf "%.3f", s/(1048576*'"$dur_s"')}')
  get_mb=$(tail -n +2 "$csv" | awk -F',' '$2=="GET" && $5==1 {s+=$6} END {printf "%.3f", s/(1048576*'"$dur_s"')}')

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$mode" "$dist" "$chunk_kb" "$rf" "$total" "$errors" \
    "$tput" "$p50" "$p95" "$p99" "$put_mb" "$get_mb" "$csv" \
    >> "$SUMMARY"

  echo "  ops=$total errors=$errors tput=${tput}ops/s p50=${p50}us p95=${p95}us p99=${p99}us"
}

MODES=(baseline adaptive)
DISTS=(hot_cold zipfian uniform)
CHUNK_KBS=(4 64 256)
RFS_ADAPTIVE=(2 3 5)
RF_BASELINE=3

echo "===== NimbusVault Benchmark Matrix ====="
echo "meta=$META_ADDR  ops=$OPS  chunks=$CHUNKS  threads=$THREADS"
echo ""

for mode in "${MODES[@]}"; do
  for dist in "${DISTS[@]}"; do
    for chunk_kb in "${CHUNK_KBS[@]}"; do
      if [[ "$mode" == "adaptive" ]]; then
        for rf in "${RFS_ADAPTIVE[@]}"; do
          run_one "$mode" "$dist" "$chunk_kb" "$rf"
        done
      else
        run_one "$mode" "$dist" "$chunk_kb" "$RF_BASELINE"
      fi
    done
  done
done

echo ""
echo "===== Done. Results in $RESULTS_DIR ====="
echo "Summary TSV: $SUMMARY"
echo "Run bench/analysis/plot_results.py to generate plots."
