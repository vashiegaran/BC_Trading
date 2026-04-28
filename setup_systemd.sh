#!/usr/bin/env bash
# setup_systemd.sh — install/replace a systemd service for the BC_Trading bot.
#
# Usage (run on the Linux server as root):
#   sudo bash setup_systemd.sh                          # uses defaults below
#   sudo bash setup_systemd.sh bcbot /root/BC_Trading
#   sudo bash setup_systemd.sh bcbot /root/BC_Trading solana-memecoin-bot
#
# Idempotent: safe to re-run. Stops any existing instance, kills stragglers,
# clears the lock file, rewrites the unit, reloads systemd, enables & starts.

set -euo pipefail

SERVICE_NAME="${1:-bcbot}"
REPO_DIR="${2:-/root/BC_Trading}"
BINARY_NAME="${3:-solana-memecoin-bot}"

UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
LOG_PATH="/var/log/${SERVICE_NAME}.log"
ENV_FILE="${REPO_DIR}/.env"
BIN_PATH="${REPO_DIR}/target/release/${BINARY_NAME}"
LOCK_DIR="/tmp/bc_trading_locks"

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
ok()   { printf '\033[32m[OK]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[!]\033[0m %s\n' "$*"; }
err()  { printf '\033[31m[X]\033[0m %s\n' "$*" >&2; }

if [[ $EUID -ne 0 ]]; then
  err "Run as root (use sudo)."; exit 1
fi

bold "== Setup: ${SERVICE_NAME} =="
echo "  repo:    ${REPO_DIR}"
echo "  binary:  ${BIN_PATH}"
echo "  env:     ${ENV_FILE}"
echo "  unit:    ${UNIT_PATH}"
echo "  log:     ${LOG_PATH}"
echo

# 1. Sanity checks
[[ -d "$REPO_DIR" ]] || { err "Repo dir not found: $REPO_DIR"; exit 1; }
[[ -f "$ENV_FILE" ]] || { err ".env not found: $ENV_FILE"; exit 1; }
if [[ ! -x "$BIN_PATH" ]]; then
  err "Binary not found or not executable: $BIN_PATH"
  echo "  Build it first:  cd $REPO_DIR && cargo build --release"
  exit 1
fi
ok "All paths exist"

# 2. Strip 'export ' prefixes from .env (systemd EnvironmentFile rejects them)
if grep -q '^export ' "$ENV_FILE"; then
  warn "Stripping 'export ' prefixes from $ENV_FILE"
  cp "$ENV_FILE" "${ENV_FILE}.bak.$(date +%s)"
  sed -i 's/^export //' "$ENV_FILE"
  ok "Backed up + stripped"
fi

# 3. Stop existing service
if systemctl list-unit-files | grep -q "^${SERVICE_NAME}.service"; then
  warn "Existing ${SERVICE_NAME} unit found — stopping"
  systemctl stop "${SERVICE_NAME}" 2>/dev/null || true
fi

# 4. Kill stray processes (tmux/nohup leftovers, or the old PID 1062)
STRAY_PIDS=$(pgrep -f "${BIN_PATH}" 2>/dev/null || true)
if [[ -n "$STRAY_PIDS" ]]; then
  warn "Killing stray processes: $STRAY_PIDS"
  echo "$STRAY_PIDS" | xargs -r kill 2>/dev/null || true
  sleep 3
  STILL_ALIVE=$(pgrep -f "${BIN_PATH}" 2>/dev/null || true)
  if [[ -n "$STILL_ALIVE" ]]; then
    warn "Force-killing: $STILL_ALIVE"
    echo "$STILL_ALIVE" | xargs -r kill -9 2>/dev/null || true
    sleep 1
  fi
fi

# 5. Kill tmux server if anything's hosting the bot there
if command -v tmux >/dev/null 2>&1; then
  if tmux ls 2>/dev/null | grep -q .; then
    warn "Active tmux sessions found — killing tmux server"
    tmux kill-server 2>/dev/null || true
  fi
fi

# 6. Clear stale wallet lock files (safe now that we killed all bot processes)
if [[ -d "$LOCK_DIR" ]]; then
  STALE=$(find "$LOCK_DIR" -type f -name '*.lock' 2>/dev/null || true)
  if [[ -n "$STALE" ]]; then
    warn "Removing stale lock files:"
    echo "$STALE" | sed 's/^/    /'
    rm -f "$LOCK_DIR"/*.lock
  fi
fi

# 7. Prepare log file
touch "$LOG_PATH"
chmod 644 "$LOG_PATH"
ok "Log file ready: $LOG_PATH"

# 8. Write the unit
cat > "$UNIT_PATH" <<EOF
[Unit]
Description=${SERVICE_NAME} (BC_Trading Solana Memecoin Bot)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=${REPO_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${BIN_PATH}
Restart=on-failure
RestartSec=10
# Give the bot time to flush state on shutdown
TimeoutStopSec=30
KillSignal=SIGTERM
StandardOutput=append:${LOG_PATH}
StandardError=append:${LOG_PATH}
# Resource hardening (light — bot is single-process)
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
ok "Wrote $UNIT_PATH"

# 9. Reload + enable + start
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}" >/dev/null 2>&1
systemctl restart "${SERVICE_NAME}"

# 10. Wait + verify
sleep 4
if systemctl is-active --quiet "${SERVICE_NAME}"; then
  ok "${SERVICE_NAME} is RUNNING"
  echo
  bold "-- status --"
  systemctl status "${SERVICE_NAME}" --no-pager -l | head -15
  echo
  bold "-- last 30 log lines --"
  tail -30 "$LOG_PATH" 2>/dev/null || true
  echo
  bold "Daily commands:"
  echo "  systemctl status ${SERVICE_NAME}"
  echo "  systemctl restart ${SERVICE_NAME}    # after rebuild"
  echo "  systemctl stop ${SERVICE_NAME}"
  echo "  tail -f ${LOG_PATH}                 # live logs"
  echo "  journalctl -u ${SERVICE_NAME} -f    # alt live logs"
else
  err "${SERVICE_NAME} failed to start"
  echo
  bold "-- status --"
  systemctl status "${SERVICE_NAME}" --no-pager -l || true
  echo
  bold "-- journal --"
  journalctl -xeu "${SERVICE_NAME}" --no-pager | tail -40 || true
  exit 1
fi
