#!/bin/bash
# watchdog.sh — Keeps deals-hunt server alive locally.
# Run every 5 minutes via cron.

DEALS_DIR="/home/rawrben/projects/deals_hunt"
WATCHDOG_LOG="/tmp/watchdog.log"
LOCK="/tmp/watchdog.lock"

# Prevent concurrent runs
exec 9>"$LOCK"
flock -n 9 || { echo "watchdog already running, skipping"; exit 0; }

log() { echo "[$(TZ=America/Toronto date '+%Y-%m-%d %H:%M:%S EST')] $*" >> "$WATCHDOG_LOG"; }
log "=== watchdog start ==="

start_server() {
    pkill -f "server\.py" 2>/dev/null; sleep 2
    cd "$DEALS_DIR" && git pull origin main >>/tmp/server.log 2>&1 && log "git pull done"
    cd "$DEALS_DIR" && nohup python3 server.py 9>&- >>/tmp/server.log 2>&1 &
    log "server.py started (PID $!)"
    sleep 8
}

# ── 1. Ensure server is healthy ──────────────────────────────────────────────
STATUS=$(curl -sf --max-time 12 http://localhost:8080/api/status 2>/dev/null || echo "")
READY=$(echo "$STATUS" | python3 -c "
import sys, json
try:
    print(json.load(sys.stdin).get('ready', False))
except:
    print(False)
" 2>/dev/null || echo "False")

if [[ "$READY" != "True" ]]; then
    log "Server not ready (got: '$READY') — restarting"
    start_server
else
    log "Server OK"
fi

# ── 2. Check deals freshness (stale > 15 min → restart to force refresh) ────
STALENESS=$(echo "$STATUS" | python3 -c "
import sys, json, time
from datetime import datetime
try:
    d = json.load(sys.stdin)
    fetched = d.get('fetched', '')
    if fetched:
        ts = datetime.fromisoformat(fetched.replace('Z', '+00:00')).timestamp()
        print(int(time.time() - ts))
    else:
        print(9999)
except:
    print(9999)
" 2>/dev/null || echo "9999")

if [[ "$STALENESS" -gt 900 ]]; then
    log "Deals stale (${STALENESS}s old) — restarting server to force refresh"
    start_server
fi

log "=== watchdog done ==="
