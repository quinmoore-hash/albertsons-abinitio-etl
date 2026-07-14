#!/bin/ksh
#############################################################################
# notify.ksh
# Failure notification hook invoked by nightly_batch.plan.
# Usage: notify.ksh <email> <slack-channel>
#############################################################################
EMAIL=${1:?"usage: notify.ksh <email> <slack-channel>"}
CHANNEL=${2:-"#store-data-ops"}

SUBJECT="[Albertsons ETL] nightly_batch task failure"
BODY="A task in nightly_batch failed at $(date '+%Y-%m-%d %H:%M:%S'). Check logs under $PROJECT_DIR/logs."

# email
echo "$BODY" | mailx -s "$SUBJECT" "$EMAIL" 2>/dev/null || \
  echo "[notify] (demo) would email $EMAIL: $SUBJECT"

# slack
if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
  curl -sf -X POST -H 'Content-type: application/json' \
    --data "{\"channel\":\"$CHANNEL\",\"text\":\"$SUBJECT - $BODY\"}" \
    "$SLACK_WEBHOOK_URL" >/dev/null || true
else
  echo "[notify] (demo) would post to $CHANNEL: $SUBJECT"
fi
