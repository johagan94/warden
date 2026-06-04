#!/bin/sh
# ==========================================================================
# Warden extra: Decypharr Janitor — clean array-stop orchestrator
# --------------------------------------------------------------------------
# OPTIONAL. For Unraid users running decypharr with a FUSE mount. Ensures a
# clean array stop so the FUSE mount + DB do not leave the array "busy" and
# block/dirty the shutdown.
#
# Install: Unraid -> Settings -> User Scripts -> add this script,
#          schedule = "At Stopping of Array". (Or call it from your own
#          array-stop hook.) Not a cron job.
#
# Adapted from BinsonBuzz unRAID-rclone-mounting-scripts for the decypharr
# FUSE setup. Deliberately does NOT export ZFS pools (that is environment-
# specific and dangerous on a stock Unraid array). Enable ZFS only if you
# know you need it and set ZFS_POOLS below.
# ==========================================================================

# ----------------------------- CONFIG -------------------------------------
FUSE_MOUNTS="/mnt/cache/data/dfs"                 # decypharr FUSE mount(s), space-separated
STOP_CONTAINERS="decypharr"                        # stop these first (mount owners)
DB_CONTAINERS=""                                   # e.g. "postgresql16" — flushed/stopped before unmount
ZFS_POOLS=""                                        # LEAVE EMPTY on stock Unraid. Only set if you truly export ZFS pools.
LOG="/tmp/decypharr-janitor.log"
# --------------------------------------------------------------------------

log() { echo "$(date "+%H:%M:%S") $1" | tee -a "$LOG"; }

log "--- decypharr Janitor: clean stop starting ---"

# 1. flush RAM buffers to disk
sync; log "RAM buffers synced"

# 2. stop DB containers cleanly first (so they checkpoint, not get yanked)
for c in $DB_CONTAINERS; do
    if [ "$(docker inspect -f "{{.State.Running}}" "$c" 2>/dev/null)" = "true" ]; then
        docker stop "$c" >/dev/null 2>&1 && log "stopped DB container $c"
    fi
done

# 3. stop the mount-owning containers (decypharr) so the FUSE fs is released
for c in $STOP_CONTAINERS; do
    if [ "$(docker inspect -f "{{.State.Running}}" "$c" 2>/dev/null)" = "true" ]; then
        docker stop "$c" >/dev/null 2>&1 && log "stopped $c"
    fi
done

# 4. lazy-unmount the FUSE mount(s) so nothing holds the array busy
for m in $FUSE_MOUNTS; do
    if mountpoint -q "$m" 2>/dev/null; then
        umount -l "$m" >/dev/null 2>&1
        fusermount -uz "$m" >/dev/null 2>&1 || fusermount3 -uz "$m" >/dev/null 2>&1
        log "lazy-unmounted $m"
    fi
done

# 5. OPTIONAL ZFS export (disabled unless ZFS_POOLS is set)
for pool in $ZFS_POOLS; do
    if zpool export -f "$pool" >/dev/null 2>&1; then
        log "exported ZFS pool $pool"
    else
        log "WARNING: ZFS pool $pool busy — manual check may be needed"
    fi
done

sync; log "--- decypharr Janitor: done ---"
exit 0
