#!/usr/bin/env bash
set -euo pipefail

CLI="./build/nimbus_cli"
META="127.0.0.1:9100"
WATCH_FILE="/tmp/nimbusvault/demo_chunks.txt"

for arg in "$@"; do
  case $arg in
    --meta=*) META="${arg#*=}" ;;
    --meta)   shift; META="$1" ;;
  esac
done
TRANS_DIR="/tmp/nimbusvault/trans_log"
mkdir -p "$TRANS_DIR"

RESET='\033[0m'
BOLD='\033[1m'
DIM='\033[2m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
BG_NAVY='\033[48;5;17m'

rf_color() {
  case "$1" in
    5) printf '%s' "${GREEN}${BOLD}" ;;
    3) printf '%s' "${YELLOW}${BOLD}" ;;
    2) printf '%s' "${RED}${BOLD}" ;;
    *) printf '%s' "${WHITE}" ;;
  esac
}

tier_label() {
  case "$1" in
    5) echo "[HOT]" ;;
    3) echo "[WRM]" ;;
    2) echo "[CLD]" ;;
    *) echo "[---]" ;;
  esac
}


while true; do
  COLS=$(tput cols 2>/dev/null || echo 80)
  clear

  # ── header ────────────────────────────────────────────────────────────────
  printf "${BG_NAVY}${WHITE}${BOLD}%-${COLS}s${RESET}\n" "  NIMBUSVAULT — LIVE REPLICA MONITOR"
  printf "${BG_NAVY}${WHITE}${BOLD}%-${COLS}s${RESET}\n" "  Updates every 1s  |  $(date '+%H:%M:%S')"
  echo ""

  # ── column headers ────────────────────────────────────────────────────────
  printf "${BOLD}${WHITE}  %-22s  %-8s  %-7s  %-15s  %s${RESET}\n" \
    "CHUNK" "RF" "TIER" "STATE" "REPLICAS"
  printf '  '
  printf '%*s' "$((COLS - 4))" '' | tr ' ' '─'
  printf '\n'

  # ── data rows ─────────────────────────────────────────────────────────────
  if [[ ! -f "$WATCH_FILE" ]] || [[ ! -s "$WATCH_FILE" ]]; then
    echo -e "  ${DIM}Waiting for demo.sh to register chunks...${RESET}"
  else
    while IFS= read -r chunk; do
      [[ -z "$chunk" ]] && continue

      info=$("$CLI" --meta "$META" info "$chunk" 2>/dev/null) || {
        printf '  %-22s  %s\n' "$chunk" "(not yet created)"
        continue
      }

      rf=$(echo    "$info" | grep "^rf:"    | awk '{print $2}')
      state=$(echo "$info" | grep "^state:" | awk '{print $2}')
      replicas=$(echo "$info" | awk '/^replicas:/{f=1;next} /^old_replicas/{f=0} f && /^  sn/{id=$1; sub(/=.*/,"",id); printf "%s,",id}' | sed 's/,$//')

      col=$(rf_color   "$rf")
      tier=$(tier_label "$rf")

      # Timestamp file for sticky TRANSITIONING highlight
      marker="$TRANS_DIR/$(echo "$chunk" | tr -cs 'a-zA-Z0-9' '_')"

      if [[ "$state" == "TRANSITIONING" ]]; then
        date +%s > "$marker"
        printf "${YELLOW}${BOLD}  %-22s  %-8s  %-7s  >> %-12s  %s${RESET}\n" \
          "$chunk" "RF=${rf}" "$tier" "$state" "$replicas"

      elif [[ -f "$marker" ]] && (( $(date +%s) - $(cat "$marker") < 6 )); then
        # Recently settled — keep it visible for 6 seconds after TRANSITIONING
        printf "${CYAN}${BOLD}  %-22s  %-8s  %-7s  %-15s  %s${RESET}\n" \
          "$chunk" "RF=${rf}" "$tier" "[ SETTLED ]" "$replicas"

      else
        [[ -f "$marker" ]] && rm -f "$marker"
        printf '  %-22s  '   "$chunk"
        printf "${col}%-8s${RESET}  " "RF=${rf}"
        printf "${col}%-7s${RESET}  " "$tier"
        printf "${GREEN}%-15s${RESET}  " "$state"
        printf "${DIM}%s${RESET}\n" "$replicas"
      fi

    done < "$WATCH_FILE"
  fi

  # ── footer ────────────────────────────────────────────────────────────────
  echo ""
  printf '  '
  printf '%*s' "$((COLS - 4))" '' | tr ' ' '─'
  printf '\n'
  echo -e "  ${DIM}RF=5 [HOT] — popular   RF=3 [WRM] — normal   RF=2 [CLD] — quiet${RESET}"
  echo -e "  ${DIM}TRANSITIONING = replica move in progress — reads still succeed${RESET}"
  echo ""
  echo -e "  ${CYAN}Watching:${RESET} $META   ${DIM}(Ctrl+C to stop)${RESET}"

  sleep 1
done
