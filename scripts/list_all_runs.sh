#!/usr/bin/env bash
set -euo pipefail
API_URL="${API_URL:-http://localhost:8080}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq not found; pretty-print disabled (brew install jq recommended)."
  JQ="cat"
else
  JQ="jq ."
fi

JOBS_JSON="$(curl -sS "${API_URL}/v1/jobs")"
echo "==> Jobs:"; echo "$JOBS_JSON" | ${JQ}; echo

echo "$JOBS_JSON" | jq -r '.jobs[].id' 2>/dev/null | while read -r JID; do
  echo "==> Runs for job ${JID}"
  curl -sS "${API_URL}/v1/jobs/${JID}/runs" | ${JQ}; echo
done
