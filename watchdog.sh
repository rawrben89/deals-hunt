#!/bin/bash
# watchdog.sh — Keeps deals-hunt server and cloudflared tunnel alive.
# Run every 5 minutes via cron.

DEALS_DIR="/home/rawrben/projects/deals_hunt"
CLOUDFLARED_BIN="/home/rawrben/cloudflared"
CF_LOG="/tmp/cloudflared.log"
INDEX_HTML="$DEALS_DIR/index.html"
WATCHDOG_LOG="/tmp/watchdog.log"
LOCK="/tmp/watchdog.lock"
GIT_REMOTE="origin"

# Prevent concurrent runs
exec 9>"$LOCK"
flock -n 9 || { echo "watchdog already running, skipping"; exit 0; }

log() { echo "[$(TZ=America/Toronto date '+%Y-%m-%d %H:%M:%S EST')] $*" >> "$WATCHDOG_LOG"; }
log "=== watchdog start ==="

start_server() {
    pkill -f "server\.py" 2>/dev/null; sleep 2
    cd "$DEALS_DIR" && git pull origin main >>/tmp/server.log 2>&1 && log "git pull done"
    cd "$DEALS_DIR" && nohup python3 server.py >>/tmp/server.log 2>&1 &
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
    STATUS=$(curl -sf --max-time 12 http://localhost:8080/api/status 2>/dev/null || echo "")
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

# ── 3. Check cloudflared and tunnel URL ──────────────────────────────────────
CURRENT_URL=$(grep -oP 'https://[a-z0-9-]+\.trycloudflare\.com' "$INDEX_HTML" 2>/dev/null | head -1 || echo "")
NEED_CF_RESTART=0

if ! pgrep -f "cloudflared.*tunnel" >/dev/null; then
    log "cloudflared not running"
    NEED_CF_RESTART=1
elif [[ -n "$CURRENT_URL" ]]; then
    HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" --max-time 18 "$CURRENT_URL/api/status" 2>/dev/null || echo "0")
    if [[ "$HTTP_CODE" == "200" ]]; then
        log "Tunnel OK: $CURRENT_URL"
    else
        log "Tunnel dead ($CURRENT_URL → HTTP $HTTP_CODE)"
        NEED_CF_RESTART=1
    fi
else
    log "No tunnel URL in index.html"
    NEED_CF_RESTART=1
fi

if [[ $NEED_CF_RESTART -eq 1 ]]; then
    log "Restarting cloudflared..."
    pkill -f "cloudflared.*tunnel" 2>/dev/null; sleep 2
    truncate -s0 "$CF_LOG"
    nohup "$CLOUDFLARED_BIN" tunnel --url http://localhost:8080 --logfile "$CF_LOG" >>/tmp/cf_out.log 2>&1 &
    log "cloudflared restarted — waiting for URL (up to 45s)"

    NEW_URL=""
    for i in $(seq 1 45); do
        sleep 1
        NEW_URL=$(grep -oP 'https://[a-z0-9-]+\.trycloudflare\.com' "$CF_LOG" 2>/dev/null | head -1 || echo "")
        [[ -n "$NEW_URL" ]] && break
    done

    if [[ -n "$NEW_URL" ]]; then
        log "Got URL: $NEW_URL"
        if [[ "$NEW_URL" != "$CURRENT_URL" ]]; then
            sed -i "s|https://[a-z0-9-]*\.trycloudflare\.com|$NEW_URL|g" "$INDEX_HTML"
            cd "$DEALS_DIR"
            git add index.html
            git commit -m "watchdog: update tunnel URL to $NEW_URL"
            git push "$GIT_REMOTE" main
            log "index.html updated and pushed (was: $CURRENT_URL)"
        else
            log "URL unchanged: $NEW_URL"
        fi
    else
        log "WARNING: no URL in cloudflared log after 45s"
    fi
fi

log "=== watchdog done ==="
