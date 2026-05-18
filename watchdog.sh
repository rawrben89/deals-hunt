#!/bin/bash
# watchdog.sh — Keeps deals-hunt server and cloudflared tunnel alive.
# Run every 5 minutes via cron.

DEALS_DIR="/home/rawrben/projects/deals_hunt"
WATCHDOG_LOG="/tmp/watchdog.log"
LOCK="/tmp/watchdog.lock"
CLOUDFLARED="/home/rawrben/cloudflared"
CF_LOG="/tmp/cloudflared.log"

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

# ── 3. Keep cloudflared tunnel alive ─────────────────────────────────────────
update_tunnel_url() {
    local NEW_URL="$1"
    # Update api-url.json so GitHub Pages frontend discovers the new backend
    echo "{\"url\": \"${NEW_URL}\"}" > "$DEALS_DIR/api-url.json"
    cd "$DEALS_DIR"
    git add api-url.json
    if ! git diff --cached --quiet; then
        git commit -m "chore: update tunnel URL to $NEW_URL [skip ci]"
        git push origin main >> /tmp/watchdog_git.log 2>&1 && log "Pushed new tunnel URL to GitHub" \
            || log "WARNING: git push failed"
    fi
}

start_tunnel() {
    pkill -f "cloudflared" 2>/dev/null; sleep 2
    nohup "$CLOUDFLARED" tunnel --url http://localhost:8080 --no-autoupdate \
        >> "$CF_LOG" 2>&1 &
    log "cloudflared started (PID $!)"
    sleep 8
    NEW_URL=$(grep -oP 'https://[a-z0-9-]+\.trycloudflare\.com' "$CF_LOG" | tail -1)
    if [[ -n "$NEW_URL" ]]; then
        update_tunnel_url "$NEW_URL"
        log "Tunnel URL updated to $NEW_URL"
    else
        log "WARNING: could not extract tunnel URL from log"
    fi
}

if ! pgrep -f "cloudflared" > /dev/null 2>&1; then
    log "cloudflared not running — starting tunnel"
    truncate -s 0 "$CF_LOG"
    start_tunnel
else
    # Verify the tunnel URL in api-url.json is still live
    CURRENT_URL=$(python3 -c "import json; print(json.load(open('$DEALS_DIR/api-url.json')).get('url',''))" 2>/dev/null || echo "")
    if [[ -n "$CURRENT_URL" ]]; then
        HTTP_CODE=$(curl -sf --max-time 8 -o /dev/null -w "%{http_code}" "$CURRENT_URL/" 2>/dev/null || echo "000")
        if [[ "$HTTP_CODE" != "200" ]]; then
            log "Tunnel URL $CURRENT_URL dead ($HTTP_CODE) — restarting"
            truncate -s 0 "$CF_LOG"
            start_tunnel
        else
            log "Tunnel OK ($CURRENT_URL)"
        fi
    fi
fi

log "=== watchdog done ==="
