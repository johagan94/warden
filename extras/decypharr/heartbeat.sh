#!/bin/sh
# ==========================================================================
# Warden extra: Decypharr FUSE mount Heartbeat / stale-mount auto-recovery
# --------------------------------------------------------------------------
# OPTIONAL. Only relevant if you use decypharr (https://github.com/sirrobot01/decypharr)
# with its FUSE/DFS mount. Generic *Arr users do NOT need this.
#
# Decypharr serves media through a FUSE mount (default /mnt/cache/data/dfs).
# If decypharr crashes/OOMs, that mount goes stale ("Transport endpoint is
# not connected") and the whole library reads as broken until someone
# manually unmounts + restarts. Decypharr only clears a stale mount at
# startup, so a *running* container whose mount died is not self-healed.
#
# This watchdog detects a stale/hung mount and recovers it: fusermount -uz
# + docker restart, with a startup grace, a cooldown, and a loop-guard so it
# can never thrash. Host-level — does NOT modify decypharr.
#
# Install: run ./enable.sh (adds it to cron every 3 min), or add manually.
# DISABLE: ./enable.sh --disable, or just remove the cron line.
# ==========================================================================

# ----------------------------- CONFIG -------------------------------------
MOUNT="/mnt/cache/data/dfs"          # decypharr FUSE mount (host path)
PROBE="$MOUNT/__all__"               # a path that only resolves when mounted
CONTAINER="decypharr"                # decypharr container name
PROBE_TIMEOUT=15                     # seconds before a hung stat = stale
COOLDOWN=300                         # min seconds between recovery attempts
MIN_UPTIME=120                       # ignore a container started < this ago
MAX_FAILS=5                          # pause auto-recovery after N tries in window
FAIL_WINDOW=3600

# Opt-in: pause the *Arrs while the mount is down so they do not flag the
# whole library as missing, then resume them after recovery. OFF by default.
PAUSE_ARRS="N"                       # Y to enable
ARR_CONTAINERS="sonarr radarr lidarr"   # only used when PAUSE_ARRS=Y

STATE="/tmp/decypharr-heartbeat.state"
LOG="/tmp/decypharr-heartbeat.log"
# --------------------------------------------------------------------------

log() { echo "$(date "+%Y-%m-%dT%H:%M:%S%z") $1" >> "$LOG"; }
now=$(date +%s)

running=$(docker inspect -f "{{.State.Running}}" "$CONTAINER" 2>/dev/null)
[ "$running" = "true" ] || exit 0   # not running = operator/Unraid managed; do not fight it

started=$(docker inspect -f "{{.State.StartedAt}}" "$CONTAINER" 2>/dev/null)
started_epoch=$(date -d "$started" +%s 2>/dev/null || echo 0)
if [ "$started_epoch" -gt 0 ]; then
    [ $((now - started_epoch)) -lt "$MIN_UPTIME" ] && exit 0
fi

if timeout "$PROBE_TIMEOUT" stat "$PROBE" >/dev/null 2>&1; then
    echo "fails=0 last=0" > "$STATE"   # healthy -> reset
    exit 0
fi

fails=0; last=0
[ -f "$STATE" ] && . "$STATE" 2>/dev/null

if [ $((now - last)) -lt "$COOLDOWN" ]; then
    log "STALE but within cooldown ($((now-last))s < ${COOLDOWN}s); skip"; exit 0
fi
if [ "$fails" -ge "$MAX_FAILS" ] && [ $((now - last)) -lt "$FAIL_WINDOW" ]; then
    log "CRITICAL: $fails recovery attempts in window — auto-recovery PAUSED, manual fix needed"; exit 0
fi

log "STALE mount at $MOUNT — recovering (attempt $((fails+1)))"
if [ "$PAUSE_ARRS" = "Y" ]; then
    for a in $ARR_CONTAINERS; do docker pause "$a" >/dev/null 2>&1 && log "paused $a"; done
fi
fusermount -uz "$MOUNT" 2>>"$LOG" || fusermount3 -uz "$MOUNT" 2>>"$LOG" || umount -l "$MOUNT" 2>>"$LOG"
docker restart "$CONTAINER" >>"$LOG" 2>&1
log "recovered: fusermount -uz + docker restart $CONTAINER (exit $?)"
if [ "$PAUSE_ARRS" = "Y" ]; then
    sleep 5
    for a in $ARR_CONTAINERS; do docker unpause "$a" >/dev/null 2>&1 && log "unpaused $a"; done
fi
echo "fails=$((fails+1)) last=$now" > "$STATE"
exit 0
