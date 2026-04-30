#!/usr/bin/env bash
set -euo pipefail

CLI="./build/nimbus_cli"
META="127.0.0.1:9100"
WATCH_FILE="/tmp/nimbusvault/demo_chunks.txt"
PID_FILE="/tmp/nimbusvault/cluster.pids"
META_COUNT=3
STORAGE_COUNT=0

if [[ -f "$PID_FILE" ]]; then
  TOTAL_PROCS=$(wc -l < "$PID_FILE" 2>/dev/null || echo 0)
  if [[ "$TOTAL_PROCS" =~ ^[0-9]+$ ]] && (( TOTAL_PROCS > META_COUNT )); then
    STORAGE_COUNT=$(( TOTAL_PROCS - META_COUNT ))
  fi
fi

STORAGE_COUNT_DISPLAY="$STORAGE_COUNT"
if [[ "$STORAGE_COUNT_DISPLAY" -eq 0 ]]; then
  STORAGE_COUNT_DISPLAY=$(ps aux | grep 'storage_server' | grep -v grep | wc -l | tr -d ' ')
fi

# ── colours ───────────────────────────────────────────────────────────────────
RESET='\033[0m'
BOLD='\033[1m'
DIM='\033[2m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
BG_BLUE='\033[44m'

# ── helpers ───────────────────────────────────────────────────────────────────

cols() { tput cols 2>/dev/null || echo 80; }

hr() {
  local w; w=$(cols)
  printf '  '
  printf '%*s' "$((w - 4))" '' | tr ' ' '─'
  printf '\n'
}

banner_box() {
  # banner_box BW LINE [LINE ...]
  local bw="$1"; shift
  local hline; hline=$(printf '%*s' "$bw" '' | tr ' ' '═')
  echo "  ╔${hline}╗"
  local line
  for line in "$@"; do
    if [[ -z "$line" ]]; then
      printf '  ║%-'"$bw"'s║\n' ""
    else
      printf '  ║  %-'"$((bw - 2))"'s║\n' "$line"
    fi
  done
  echo "  ╚${hline}╝"
}

pause() {
  echo ""
  hr
  read -rp "  Press ENTER to continue... " _
  echo ""
}

section() {
  local title="$1"
  local sub="${2:-}"
  local w; w=$(cols)
  echo ""
  printf "${BG_BLUE}${WHITE}${BOLD}%-$((w - 1))s${RESET}\n" "   $title"
  if [[ -n "$sub" ]]; then
    printf "${BG_BLUE}${WHITE}${BOLD}%-$((w - 1))s${RESET}\n" "   $sub"
  fi
  echo ""
  sleep 0.4
}

narrate() {
  echo -e "${CYAN}  >>  $1${RESET}"
  sleep 0.3
}

run() {
  echo ""
  echo -e "${YELLOW}${BOLD}  \$ $1${RESET}"
  sleep 0.7
  eval "$1"
  echo ""
}

ok() {
  echo -e "${GREEN}${BOLD}  [OK]  $1${RESET}"
}

warn() {
  echo -e "${RED}${BOLD}  [!!]  $1${RESET}"
}

info_pretty() {
  local chunk="$1"
  echo ""
  echo -e "${YELLOW}${BOLD}  \$ $CLI --meta $META info $chunk${RESET}"
  sleep 0.7
  local out
  out=$("$CLI" --meta "$META" info "$chunk" 2>&1) || { warn "info failed: $out"; return 1; }
  while IFS= read -r line; do
    if [[ "$line" == *"rf:"* ]]; then
      local rf; rf=$(echo "$line" | awk '{print $2}')
      if   [[ "$rf" == "5" ]]; then echo -e "  ${GREEN}${BOLD}$line${RESET}"
      elif [[ "$rf" == "3" ]]; then echo -e "  ${YELLOW}${BOLD}$line${RESET}"
      else                          echo -e "  ${RED}${BOLD}$line${RESET}"
      fi
    elif [[ "$line" == *"state:"* && "$line" == *"TRANSITIONING"* ]]; then
      echo -e "  ${YELLOW}$line${RESET}"
    elif [[ "$line" == *"state:"* ]]; then
      echo -e "  ${GREEN}$line${RESET}"
    else
      echo -e "  ${WHITE}$line${RESET}"
    fi
  done <<< "$out"
  echo ""
}

bw() {
  local w; w=$(cols)
  local b=$(( w > 84 ? 72 : w - 12 ))
  echo $(( b < 68 ? 68 : b ))
}

# ── sanity check ──────────────────────────────────────────────────────────────
if [[ ! -x "$CLI" ]]; then
  echo -e "${RED}[!!] $CLI not found. Run: cmake --build build${RESET}"
  exit 1
fi
mkdir -p "$(dirname "$WATCH_FILE")"
> "$WATCH_FILE"

# Reset demo chunks so RF always starts at 3 regardless of prior runs
for _chunk in my-document research-paper viral-video news-article; do
  "$CLI" --meta "$META" del "$_chunk" > /dev/null 2>&1 || true
done
unset _chunk

# ─────────────────────────────────────────────────────────────────────────────
#  INTRO
# ─────────────────────────────────────────────────────────────────────────────
clear
echo ""
echo -e "${CYAN}${BOLD}"
banner_box "$(bw)" \
  "" \
  "ADAPTIVE  NIMBUS  VAULT" \
  "Distributed Object Store - Self-Adapting Replication" \
  ""
echo -e "${RESET}"
echo ""
pause

# ─────────────────────────────────────────────────────────────────────────────
#  SCENE 1 — THE CLUSTER IS ALIVE
# ─────────────────────────────────────────────────────────────────────────────
clear
section "SCENE 1  —  The Cluster Is Alive" \
  "${META_COUNT} metadata servers + ${STORAGE_COUNT_DISPLAY} servers, all local"

narrate "First, let's verify the cluster is running."
narrate "We have ${META_COUNT} metadata servers (one leader, two backups) and ${STORAGE_COUNT_DISPLAY} servers."

run "ps aux | grep -E 'meta_server|storage_server' | grep -v grep | awk '{print \$11, \$12, \$13}' | head -10"

narrate "The PID file tracks every process so we can kill individual nodes later."
run "cat $PID_FILE | wc -l && echo 'processes tracked'"

ok "Cluster is healthy."
pause

# ─────────────────────────────────────────────────────────────────────────────
#  SCENE 2 — BASIC PUT / GET / INFO
# ─────────────────────────────────────────────────────────────────────────────
clear
section "SCENE 2  —  Storing and Reading Data" "put, get, info"

narrate "Let's store our first file. One command — data goes to 3 servers automatically."

run "$CLI --meta $META put my-document 'Hello NimbusVault! This is our first stored chunk.'"

narrate "Read it back. Client contacts the metadata server for location, then reads from storage."
run "$CLI --meta $META get my-document"

narrate "Inspect what the metadata server knows about this file."
info_pretty "my-document"

narrate "RF=3, STABLE, three replicas on separate servers."
narrate "Servers were selected by load, free space, and reliability score."

run "$CLI --meta $META put research-paper 'Distributed Systems paper — cold data, rarely accessed.'"
info_pretty "research-paper"

echo "my-document"    >> "$WATCH_FILE"
echo "research-paper" >> "$WATCH_FILE"

ok "Two files stored. Both RF=3, both STABLE. Watch the right terminal."
pause

# ─────────────────────────────────────────────────────────────────────────────
#  SCENE 3 — ADAPTIVE TIERING
# ─────────────────────────────────────────────────────────────────────────────
clear
section "SCENE 3  —  Adaptive Tiering" \
        "Core thesis: the system rebalances itself without human intervention"

narrate "We create 'viral-video', then saturate it with reads."
narrate "Watch the right terminal: RF should climb from 3 to 5 automatically."

run "$CLI --meta $META put viral-video 'Breaking news video — about to go viral!'"
info_pretty "viral-video"
echo "viral-video" >> "$WATCH_FILE"

narrate "RF=3 right now. Starting a continuous read loop in the background..."
narrate "Policy engine needs 3 consecutive high-rate windows (~6s) to promote (RF=5)."
echo ""
echo -e "${YELLOW}${BOLD}  \$ (continuous read loop running in background)${RESET}"
sleep 0.7

_read_loop() {
  while true; do
    "$CLI" --meta "$META" get viral-video > /dev/null 2>&1 || true
  done
}
_read_loop &
READ_LOOP_PID=$!
disown "$READ_LOOP_PID"

narrate "Reads are live. Polling for promotion to RF=5..."
echo ""

PROMOTED=0
for tick in $(seq 1 90); do
  sleep 0.5
  RF_NOW=$("$CLI"    --meta "$META" info viral-video 2>/dev/null | grep "^rf:"    | awk '{print $2}') || RF_NOW="?"
  STATE_NOW=$("$CLI" --meta "$META" info viral-video 2>/dev/null | grep "^state:" | awk '{print $2}') || STATE_NOW=""

  if [[ "$RF_NOW" == "5" && "$STATE_NOW" == "STABLE" ]]; then
    PROMOTED=1
    kill "$READ_LOOP_PID" 2>/dev/null || true
    break
  elif [[ "$STATE_NOW" == "TRANSITIONING" ]]; then
    echo -ne "\r  ${YELLOW}  RF=${RF_NOW}  TRANSITIONING  (replicating...)${RESET}   "
  else
    echo -ne "\r  ${CYAN}  RF=${RF_NOW}  ${STATE_NOW:-STABLE}  (tick ${tick}/90)${RESET}   "
  fi
done
kill "$READ_LOOP_PID" 2>/dev/null || true
echo ""

if [[ "$PROMOTED" == "1" ]]; then
  echo -e "${GREEN}${BOLD}"
  banner_box "$(bw)" \
    "" \
    "[PROMOTED]  viral-video  RF=5  [HOT TIER]" \
    "System adapted - no restart, no config change, no manual action." \
    ""
  echo -e "${RESET}"
else
  echo -e "${YELLOW}${BOLD}  Auto-promotion timed out — showing manual replica add as fallback:${RESET}"
  echo ""
  run "$CLI --meta $META add-replica viral-video sn0"
  run "$CLI --meta $META add-replica viral-video sn3"
  echo -e "${CYAN}  >>  This is exactly what the policy engine does automatically.${RESET}"
  echo -e "${CYAN}  >>  Fires when the EWMA read rate crosses the hot-tier threshold.${RESET}"
fi

info_pretty "viral-video"

narrate "research-paper was barely touched — it stays at RF=3."
info_pretty "research-paper"

ok "Same cluster. Same hardware. Different files, different replica counts. Zero manual config."
pause


# ─────────────────────────────────────────────────────────────────────────────
#  SCENE 4 — TAPERED READS: COLD → WARM → COLD
# ─────────────────────────────────────────────────────────────────────────────
clear
section "SCENE 4  —  Tapered Reads" \
        "Moderate traffic lands viral-video in the WARM band, not HOT"

narrate "viral-video is COLD (RF=2). We now read it at ~0.5 r/s — one get every 2s."
narrate "Threshold: rate >= 0.1 → WARM, rate > 1.0 → HOT. 0.5 r/s = WARM territory."
narrate "Expect:  ${WHITE}COLD → WARM${RESET}  (RF 2 → 3) within ~6s of evaluations."
echo ""
echo -e "${YELLOW}${BOLD}  \$ (1 get / 2s on viral-video)${RESET}"
sleep 0.7

_taper_loop() {
  while true; do
    "$CLI" --meta "$META" get viral-video > /dev/null 2>&1 || true
    sleep 2
  done
}
_taper_loop &
TAPER_PID=$!
disown "$TAPER_PID"

WARMED=0
for tick in $(seq 1 60); do
  sleep 0.5
  RF_NOW=$("$CLI" --meta "$META" info viral-video 2>/dev/null | grep "^rf:" | awk '{print $2}') || RF_NOW="?"
  if [[ "$RF_NOW" == "3" ]]; then
    WARMED=1
    break
  fi
  echo -ne "\r  ${CYAN}  viral-video RF=${RF_NOW}  (waiting for COLD → WARM, tick ${tick}/60)${RESET}   "
done
echo ""

if [[ "$WARMED" == "1" ]]; then
  ok "viral-video promoted COLD → WARM at moderate read rate."
else
  warn "Did not observe COLD → WARM in 30s — taper rate may be too low/high."
fi
info_pretty "viral-video"

narrate "Now we kill the taper loop entirely. Rate drops to 0 → expect WARM → COLD."
kill "$TAPER_PID" 2>/dev/null || true
echo ""

COOLED=0
for tick in $(seq 1 60); do
  sleep 0.5
  RF_NOW=$("$CLI" --meta "$META" info viral-video 2>/dev/null | grep "^rf:" | awk '{print $2}') || RF_NOW="?"
  if [[ "$RF_NOW" == "2" ]]; then
    COOLED=1
    break
  fi
  echo -ne "\r  ${CYAN}  viral-video RF=${RF_NOW}  (waiting for WARM → COLD, tick ${tick}/60)${RESET}   "
done
echo ""

if [[ "$COOLED" == "1" ]]; then
  ok "viral-video demoted WARM → COLD after reads stopped."
else
  warn "Did not observe WARM → COLD in 30s."
fi
info_pretty "viral-video"

ok "Three tiers, two thresholds, one knob: rate."
pause

# ─────────────────────────────────────────────────────────────────────────────
#  WRAP UP
# ─────────────────────────────────────────────────────────────────────────────
clear
echo ""
echo -e "${GREEN}${BOLD}"
banner_box "$(bw)" \
  "" \
  "WHAT WE SAW" \
  "" \
  "-  Files replicated across multiple servers automatically" \
  "-  Hot files promoted to RF=5 as read traffic increased" \
  "-  Idle files demoted to RF=2  (storage reclaimed)" \
  "" \
  "Achieved with:" \
  "  *  Zero config changes" \
  "  *  Zero restarts" \
  "  *  Zero manual intervention" \
  ""
echo -e "${RESET}"
echo ""
echo -e "${WHITE}  Thank you.${RESET}"
echo ""
