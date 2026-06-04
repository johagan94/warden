#!/bin/sh
# Enable/disable the decypharr Heartbeat watchdog on Unraid (cron, every 3 min).
# Usage:  sh enable.sh            # install + activate
#         sh enable.sh --disable  # remove
# The Janitor is NOT installed here — add janitor.sh via Unraid User Scripts
# with schedule "At Stopping of Array" (see README.md).

DIR="$(cd "$(dirname "$0")" && pwd)"
CRON_FILE="/boot/config/plugins/dynamix/decypharr-heartbeat.cron"

if [ "$1" = "--disable" ]; then
    rm -f "$CRON_FILE"
    command -v update_cron >/dev/null 2>&1 && update_cron
    echo "decypharr Heartbeat disabled (removed $CRON_FILE)"
    exit 0
fi

cat > "$CRON_FILE" << EOF
# decypharr FUSE mount Heartbeat — managed by warden extras/decypharr/enable.sh
*/3 * * * * /bin/sh $DIR/heartbeat.sh >> /tmp/decypharr-heartbeat.cron.log 2>&1
EOF
chmod 644 "$CRON_FILE"
command -v update_cron >/dev/null 2>&1 && update_cron
echo "decypharr Heartbeat enabled (every 3 min) -> $CRON_FILE"
echo "Edit $DIR/heartbeat.sh to adjust mount path, cooldown, or PAUSE_ARRS."
