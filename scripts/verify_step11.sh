#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
SCHED_URL="${SCHED_URL:-http://localhost:8082}"

if ! command -v jq >/dev/null 2>&1; then JQ="cat"; else JQ="jq ."; fi

echo "==> /healthz (api)"
curl -sS "${API_URL}/healthz"; echo; echo

echo "==> List jobs"
curl -sS "${API_URL}/v1/jobs" | ${JQ}; echo

JOB_ID="$(curl -sS "${API_URL}/v1/jobs" | jq -r '.jobs[0].id' 2>/dev/null || \
         curl -sS "${API_URL}/v1/jobs" | grep -oE '"id"\s*:\s*"[^"]+"' | head -n1 | sed -E 's/.*"id"\s*:\s*"([^"]+)".*/\1/')"
echo "==> Most recent Job ID: ${JOB_ID:-<none>}"
if [[ -n "${JOB_ID:-}" ]]; then
  echo "==> List runs for most recent job"
  curl -sS "${API_URL}/v1/jobs/${JOB_ID}/runs" | ${JQ}; echo
fi

echo "==> Scheduler endpoints (role then healthz)"
ROLE_CODE="$(curl -s -o /dev/null -w '%{http_code}' "${SCHED_URL}/role" || true)"
if [[ "$ROLE_CODE" == "200" ]]; then
  echo -n "GET ${SCHED_URL}/role -> "
  curl -sS "${SCHED_URL}/role"; echo
else
  echo "GET ${SCHED_URL}/role -> ${ROLE_CODE}"
  echo -n "GET ${SCHED_URL}/healthz -> "
  curl -sS "${SCHED_URL}/healthz" || echo "error"
  echo
fi
