#!/usr/bin/env bash
# deploy.sh — pull, build, restart the bot via systemd.
#
# Usage (run on the Linux server as root):
#   sudo bash scripts/deploy.sh                     # uses defaults
#   sudo bash scripts/deploy.sh bcbot /root/BC_Trading
#
# What it does:
#   1. git pull (fast-forward only)
#   2. cargo build --release (incremental)
#   3. systemctl restart <service>
#   4. Tails the log briefly to confirm clean startup
#
# Idempotent and safe to re-run.

set -euo pipefail

SERVICE_NAME="${1:-bcbot}"
REPO_DIR="${2:-/root/BC_Trading}"
BINARY_NAME="${3:-solana-memecoin-bot}"

LOG_PATH="/var/log/${SERVICE_NAME}.log"
BIN_PATH="${REPO_DIR}/target/release/${BINARY_NAME}"

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
ok()   { printf '\033[32m[OK]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[!]\033[0m %s\n' "$*"; }
err()  { printf '\033[31m[X]\033[0m %s\n' "$*" >&2; }

if [[ $EUID -ne 0 ]]; then
  err "Run as root (use sudo)."; exit 1
fi

[[ -d "$REPO_DIR" ]] || { err "Repo dir not found: $REPO_DIR"; exit 1; }
systemctl list-unit-files | grep -q "^${SERVICE_NAME}.service" \
  || { err "Service ${SERVICE_NAME} not installed. Run setup_systemd.sh first."; exit 1; }

bold "== Deploy: ${SERVICE_NAME} =="
echo "  repo:    ${REPO_DIR}"
echo "  binary:  ${BIN_PATH}"
echo

cd "$REPO_DIR"

# 1. Git pull (fast-forward only — never auto-merge)
bold "-- git pull --"
BEFORE=$(git rev-parse --short HEAD)
if git pull --ff-only; then
  AFTER=$(git rev-parse --short HEAD)
  if [[ "$BEFORE" == "$AFTER" ]]; then
    ok "Already up to date ($AFTER)"
  else
    ok "Updated $BEFORE -> $AFTER"
    git --no-pager log --oneline "$BEFORE..$AFTER" | head -10
  fi
else
  err "git pull failed (likely uncommitted changes or non-FF). Aborting."
  exit 1
fi
echo

# 2. Build release binary (PROTOC must be on PATH or set in env)
bold "-- cargo build --release --"
BUILD_START=$(date +%s)
if cargo build --release 2>&1 | tail -8; then
  BUILD_END=$(date +%s)
  ok "Build succeeded in $((BUILD_END - BUILD_START))s"
else
  err "Build failed"
  exit 1
fi

[[ -x "$BIN_PATH" ]] || { err "Binary missing after build: $BIN_PATH"; exit 1; }
ls -la "$BIN_PATH"
echo

# 3. Restart service
bold "-- systemctl restart ${SERVICE_NAME} --"
systemctl restart "${SERVICE_NAME}"
sleep 4

if systemctl is-active --quiet "${SERVICE_NAME}"; then
  ok "${SERVICE_NAME} is RUNNING"
else
  err "${SERVICE_NAME} failed to start"
  bold "-- journal --"
  journalctl -xeu "${SERVICE_NAME}" --no-pager | tail -40 || true
  exit 1
fi
echo

# 4. Tail log briefly to confirm clean startup (look for "Config loaded successfully")
bold "-- last 30 log lines --"
sleep 2
tail -30 "$LOG_PATH" 2>/dev/null || true
echo

bold "Done. Live tail:  tail -f ${LOG_PATH}"
