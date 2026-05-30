#!/bin/bash

FLAVOR_FILE="/etc/dpf/dpuflavor.yaml"
TIMEOUT=1800
START_TIME=$(date +%s)

EXPECTED=$(grep -oP 'PF_TOTAL_SF=\K[0-9]+' "$FLAVOR_FILE" 2>/dev/null)
if [ -z "$EXPECTED" ] || [ "$EXPECTED" -eq 0 ]; then
  echo "INFO: PF_TOTAL_SF not set or is 0, no SFs expected — passing gate"
  exit 0
fi

echo "INFO: Waiting for $EXPECTED SFs to be created (timeout=${TIMEOUT}s)..."

while true; do
  ELAPSED=$(( $(date +%s) - START_TIME ))
  if [ "$ELAPSED" -ge "$TIMEOUT" ]; then
    echo "ERROR: Timed out waiting for $EXPECTED SFs after ${TIMEOUT}s"
    exit 1
  fi

  ACTUAL=$(mlnx-sf -a show -j 2>/dev/null | jq 'length // 0')
  if [ "${ACTUAL:-0}" -ge "$EXPECTED" ]; then
    echo "INFO: All SFs ready (expected=$EXPECTED, actual=$ACTUAL)"
    exit 0
  fi

  echo "INFO: SFs not ready yet (expected=$EXPECTED, actual=${ACTUAL:-0}, elapsed=${ELAPSED}s)"
  sleep 5
done
