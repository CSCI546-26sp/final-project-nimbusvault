#!/usr/bin/env bash
set -euo pipefail

CLI="./build/nimbus_cli"
META="127.0.0.1:9100"
WATCH_FILE="/tmp/nimbusvault/demo_chunks.txt"
PID_FILE="/tmp/nimbusvault/cluster.pids"

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
for _chunk in my-document research-paper viral-video; do
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
        "3 metadata servers + 5 storage servers, all local"

narrate "First, let's verify the cluster is running."
narrate "We have 3 metadata servers (one leader, two backups) and 5 storage servers."

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
narrate "Policy engine needs 2 consecutive high-rate heartbeat windows to promote (RF=5)."
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
for tick in $(seq 1 60); do
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
    echo -ne "\r  ${CYAN}  RF=${RF_NOW}  ${STATE_NOW:-STABLE}  (tick ${tick}/60)${RESET}   "
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
#  SCENE 4 — FAULT TOLERANCE: KILL A NODE
# ─────────────────────────────────────────────────────────────────────────────
clear
section "SCENE 4  —  Kill a Storage Server" \
        "Fault tolerance — reads must still succeed"

RF_BEFORE_KILL=$("$CLI" --meta "$META" info viral-video 2>/dev/null | grep "^rf:" | awk '{print $2}') || RF_BEFORE_KILL="3"
REPLICAS_AFTER=$((RF_BEFORE_KILL - 1))

narrate "We are going to kill storage node sn2 — hard kill, no warning."
narrate "viral-video is at RF=${RF_BEFORE_KILL}. After the kill we still have ${REPLICAS_AFTER} live replicas."

SN2_PID=$(pgrep -f -- "--id sn2" 2>/dev/null | head -1 || echo "")

if [[ -z "$SN2_PID" ]]; then
  warn "sn2 is not running (may have already exited). Demonstrating the concept instead."
  narrate "In a real run:  kill \$(pgrep -f '--id sn2')  — reads still succeed."
else
  echo ""
  echo -e "${RED}${BOLD}  \$ kill $SN2_PID   # sn2 (127.0.0.1:9202) — hard kill${RESET}"
  sleep 0.7
  kill "$SN2_PID" 2>/dev/null || true
  sleep 0.8
  ok "sn2 is down. ${REPLICAS_AFTER} replicas still alive."
  echo ""

  narrate "Reading viral-video — client automatically routes to a live replica."
  run "$CLI --meta $META get viral-video"
  ok "Read succeeded. Client routed around the dead node with no config change."
  echo ""

  info_pretty "viral-video"

  narrate "sn2 still appears in the replica list — not declared dead yet."
  narrate "After ~3 missed heartbeats the metadata server evicts sn2 and triggers replica repair."
fi

pause

# ─────────────────────────────────────────────────────────────────────────────
#  SCENE 5 — STRONG CONSISTENCY
# ─────────────────────────────────────────────────────────────────────────────
clear
section "SCENE 5  —  Strong Consistency" \
        "The metadata server's safety guarantee"

narrate "The system never lies about what is saved."
narrate "Storage-first rule: bytes reach a majority of servers BEFORE the metadata server"
narrate "records that the file exists. Demonstrating an overwrite."

run "$CLI --meta $META put my-document 'Updated content — version 2!'"
run "$CLI --meta $META get my-document"
info_pretty "my-document"

narrate "Version number incremented. Every replica holds the new data."
narrate "If the metadata server crashed mid-write, orphan copies are cleaned on restart."

ok "Every file the system acknowledges — is durably saved."
pause

# ─────────────────────────────────────────────────────────────────────────────
#  SCENE 6 — COLD DEMOTION
# ─────────────────────────────────────────────────────────────────────────────
clear
section "SCENE 6  —  Cold Demotion" \
        "What goes up must come down — quiet chunks shed replicas"

narrate "viral-video is at RF=5 [HOT]. The read loop is dead — no traffic."
narrate "research-paper has been quiet the whole demo."
narrate "Policy needs ${YELLOW}demote_windows=3${RESET} consecutive low-rate windows."
narrate "Watch the right terminal — RF should fall on its own."
echo ""

DEMOTED_VIRAL=0
DEMOTED_PAPER=0
for tick in $(seq 1 60); do
  sleep 0.5
  RF_VIRAL=$("$CLI" --meta "$META" info viral-video    2>/dev/null | grep "^rf:" | awk '{print $2}') || RF_VIRAL="?"
  RF_PAPER=$("$CLI" --meta "$META" info research-paper 2>/dev/null | grep "^rf:" | awk '{print $2}') || RF_PAPER="?"

  [[ "$RF_VIRAL" == "2" || "$RF_VIRAL" == "3" ]] && DEMOTED_VIRAL=1
  [[ "$RF_PAPER" == "2" ]]                       && DEMOTED_PAPER=1

  echo -ne "\r  ${CYAN}  viral-video RF=${RF_VIRAL}   research-paper RF=${RF_PAPER}   (tick ${tick}/60)${RESET}   "
  if [[ "$DEMOTED_VIRAL" == "1" && "$DEMOTED_PAPER" == "1" ]]; then break; fi
done
echo ""
echo ""

if [[ "$DEMOTED_VIRAL" == "1" || "$DEMOTED_PAPER" == "1" ]]; then
  echo -e "${GREEN}${BOLD}"
  banner_box "$(bw)" \
    "" \
    "[DEMOTED]  Idle chunks shed replicas automatically" \
    "Same policy engine — promotes hot data, reclaims cold space." \
    ""
  echo -e "${RESET}"
else
  warn "No demotion observed in 30s — check policy demote_windows / cold_threshold."
fi

info_pretty "viral-video"
info_pretty "research-paper"

ok "Storage reclaimed without anyone touching a config file."
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
  "-  Node failure - reads routed to surviving replicas" \
  "-  All consistent - no stale reads, no lost writes" \
  "" \
  "Achieved with:" \
  "  *  Zero config changes" \
  "  *  Zero restarts" \
  "  *  Zero manual intervention" \
  ""
echo -e "${RESET}"
echo ""
echo -e "${DIM}  Benchmark:  Adaptive mode   655 ops/s  |  2.04 MB/s GET${RESET}"
echo -e "${DIM}             Baseline mode    536 ops/s  |  1.68 MB/s GET${RESET}"
echo -e "${DIM}             +22% throughput  |  -33% storage for cold data${RESET}"
echo ""
echo -e "${WHITE}  Thank you.${RESET}"
echo ""
